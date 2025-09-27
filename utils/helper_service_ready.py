import time
import os

import psycopg2
from kafka import KafkaConsumer, TopicPartition
from logger import log
from typing import Optional, Dict
from config import LAKE_TYPE, PARQUET_PATH, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER, RDBMS_PASSWORD, \
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def wait_for_kafka(bootstrap, topic, timeout_sec: int = 60):
    from kafka import KafkaProducer
    """Wait until Kafka is reachable and the topic has partitions (after we create it)."""
    start = time.time()
    last_error = None
    while time.time() - start < timeout_sec:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap)
            partitions = producer.partitions_for(topic)
            producer.close()

            if partitions and len(partitions)>0:
                log.info(f"[SERVICE_READY][KAFKA] Kafka topic '{topic}' has {len(partitions)} partition(s)")
                return
        except Exception as e:
            last_error = e
        time.sleep(1.0)
    raise TimeoutError(f"Kafka not ready for topic '{topic}': {last_error}")



def wait_for_lake(timeout_sec: int = 60):
    """
    Block until the lake is reachable.
    - parquet: wait until PARQUET_PATH exists and contains at least one .parquet file (or is readable)
    - rdbms: wait until a connection can be established and SELECT 1 succeeds.
    """
    start = time.time()
    last_error = None

    if LAKE_TYPE.lower() == "parquet":
        while time.time() - start < timeout_sec:
            try:
                if os.path.isdir(PARQUET_PATH):
                    # readiness: directory exists; optionally ensure it is listable
                    files = [f for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")]
                    log.info(f"[SERVICE_READY][DATA_LAKE] PARQUET_PATH='{PARQUET_PATH}', parquet_files={len(files)}")
                    return
            except Exception as e:
                last_error = e
            time.sleep(1.0)
        raise TimeoutError(f"[SERVICE_NOT_READY][DATA_LAKE] Parquet lake not ready at '{PARQUET_PATH}': {last_error}")
    elif LAKE_TYPE.lower() == "rdbms":
        while time.time() - start < timeout_sec:
            import psycopg2
            try:
                conn = psycopg2.connect(
                    host=RDBMS_HOST,
                    port=RDBMS_PORT,
                    dbname=RDBMS_DB,
                    user=RDBMS_USER,
                    password=RDBMS_PASSWORD
                )
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.fetchone()
                cur.close()
                conn.close()
                log.info(f"[SERVICE_READY][DATA_LAKE] RDBMS connection established")
                return
            except Exception as e:
                last_error = e
                time.sleep(1.0)
        raise TimeoutError(f"[SERVICE_NOT_READY][DATA_LAKE] RDBMS lake not ready (host={RDBMS_HOST}, db={RDBMS_DB}): {last_error}")
    else:
        log.error(f"[SERVICE_NOT_READY][DATA_LAKE] Unknown LAKE_TYPE='{LAKE_TYPE}', continuing without wait")


def _sum_latest_offsets_from_progress(progress_json: dict) -> int:
    try:
        src = progress_json.get("sources", [])[0]
        latest = src.get("latestOffset", {})
        # shape is {"lake_stream": {"0": N0, "1": N1, ...}}
        per_part = list(latest.values())[0] if latest else {}
        return sum(per_part.values()) if isinstance(per_part, dict) else 0
    except Exception:
        return 0

def wait_for_stream_offset_growth(query, min_delta: int, timeout_sec: int = 60, nudge: bool = True):
    """
    Wait until the streaming query's Kafka `latestOffset` increases by >= min_delta.
    We 'nudge' the query with processAllAvailable() so a new trigger runs and progress updates.
    """
    import time
    start = time.time()
    base = None
    last_total = 0

    def _latest_total(q):
        p = q.lastProgress or {}
        return _sum_latest_offsets_from_progress(p) if p else 0

    # one initial nudge after producing
    if nudge:
        try:
            query.processAllAvailable()
        except Exception:
            pass

    while time.time() - start < timeout_sec:
        total = _latest_total(query)
        if base is None:
            base = total

        if (total - base) >= max(0, min_delta):
            log.info("[SERVICE_READY][STREAM_OFFSETS] Spark sees Kafka latestOffset grew by %s (base=%s total=%s)",
                     total - base, base, total)
            return

        # periodic nudge so progress gets refreshed
        if nudge:
            try:
                query.processAllAvailable()
            except Exception:
                pass

        time.sleep(1.0)
        last_total = total

    raise TimeoutError(
        f"[SERVICE_NOT_READY][STREAM_OFFSETS]Streaming query did not observe Kafka offset growth of "
        f"{min_delta} within {timeout_sec}s (Δdelta={(last_total - (base or 0))}, base={base}, last_total={last_total})."
    )



def _topic_end_offsets(bootstrap: str, topic: str) -> Dict[int, int]:
    """
    Return end offsets per partition for `topic`.
    Uses a no-group consumer and seek_to_end on each partition.
    """
    c = KafkaConsumer(
        bootstrap_servers=bootstrap,
        enable_auto_commit=False,
        group_id=None,
        consumer_timeout_ms=1000,
    )
    parts = c.partitions_for_topic(topic) or set()
    if not parts:
        c.close()
        return {}
    tps = [TopicPartition(topic, p) for p in parts]
    ends_map = c.end_offsets(tps)
    c.close()
    return {tp.partition: int(ends_map.get(tp, 0)) for tp in tps}

def kafka_total_end(bootstrap: Optional[str] = None, topic: Optional[str] = None) -> int:
    """Total end-offset across all partitions, now."""
    bootstrap = bootstrap or KAFKA_BOOTSTRAP_SERVERS
    topic = topic or KAFKA_TOPIC
    return sum(_topic_end_offsets(bootstrap, topic).values())

def wait_for_kafka_total_at_least(
    min_total: int,
    timeout_sec: int = 60,
    bootstrap: Optional[str] = None,
    topic: Optional[str] = None,
) -> int:
    """
    Wait until Kafka total end-offset is >= min_total. Returns the observed total.
    Use this with a *pre-produce* baseline: target = base + produced_total.
    """
    bootstrap = bootstrap or KAFKA_BOOTSTRAP_SERVERS
    topic = topic or KAFKA_TOPIC
    start = time.time()
    while time.time() - start < timeout_sec:
        per_part = _topic_end_offsets(bootstrap, topic)
        total = sum(per_part.values())
        log.debug("[ASSERT][KAFKA_OFFSETS][TICK] total=%s per_partition=%s", total, per_part)
        if total >= min_total:
            log.info("[SERVICE_READY][KAFKA_OFFSETS] '%s' reached total=%s (target=%s)", topic, total, min_total)
            return total
        time.sleep(1.0)
    raise TimeoutError(f"[SERVICE_NOT_READY][KAFKA_OFFSETS] '{topic}' did not reach total {min_total} within {timeout_sec}s")

# Backward compatible wrapper: if caller *doesn't* pass a base, we snapshot it here.
# Recommended usage: pass base_total measured *before* produce. Otherwise we still work,
# but will be sensitive to concurrent producers.
def wait_for_kafka_increase(
    min_delta: int,
    timeout_sec: int = 60,
    bootstrap: Optional[str] = None,
    topic: Optional[str] = None,
    base_total: Optional[int] = None,
) -> int:
    bootstrap = bootstrap or KAFKA_BOOTSTRAP_SERVERS
    topic = topic or KAFKA_TOPIC
    if base_total is None:
        base_total = kafka_total_end(bootstrap, topic)
        log.info("[ASSERT][KAFKA_OFFSETS][BASE] bootstrap=%s topic=%s base_total=%s",
                 bootstrap, topic, base_total)
    target = base_total + max(min_delta, 0)
    wait_for_kafka_total_at_least(target, timeout_sec, bootstrap, topic)
    return target

# def wait_for_kafka_increase(
#     min_delta: int,
#     timeout_sec: int = 60,
#     bootstrap: str | None = None,
#     topic: str | None = None,
# ) -> int:
#     """
#     Wait until Kafka end offsets for `topic` increase by >= min_delta.
#     Returns the observed increase; raises TimeoutError on timeout.
#     """
#     bootstrap = bootstrap or KAFKA_BOOTSTRAP_SERVERS
#     topic = topic or KAFKA_TOPIC
#
#     start_offsets = _topic_end_offsets(bootstrap, topic)
#     base_total = sum(start_offsets.values())
#
#     log.info(f"[ASSERT][KAFKA_OFFSETS][BASE] bootstrap={bootstrap} topic={topic} base_total={base_total} per_partition={start_offsets}")
#
#     start = time.time()
#     last_total = base_total
#
#     while time.time() - start < timeout_sec:
#         end_offsets = _topic_end_offsets(bootstrap, topic)
#         total = sum(end_offsets.values())
#         log.debug(f"[ASSERT][KAFKA_OFFSETS][TICK] total={total} Δ={total - base_total} per_partition={end_offsets}")
#
#         if total - base_total >= min_delta:
#             log.info(
#                 "[SERVICE_READY][KAFKA_OFFSETS] Topic '%s' end-offsets grew by %s (total=%s)",
#                 topic, total - base_total, total
#             )
#             return total - base_total
#         last_total = total
#         time.sleep(1.0)
#
#     raise TimeoutError(
#         f"[SERVICE_NOT_READY][KAFKA_OFFSETS] Topic '{topic}' did not grow by {min_delta} "
#         f"within {timeout_sec}s (Δ={last_total - base_total}, base={base_total}, last_total={last_total})."
#     )

