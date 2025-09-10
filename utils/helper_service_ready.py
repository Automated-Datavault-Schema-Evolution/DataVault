import time
import os

import psycopg2
from kafka import KafkaConsumer, TopicPartition
from logger import log

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

def wait_for_stream_offset_growth(query, min_delta: int, timeout_sec: int = 60):
    """Wait until Spark's streaming query reports that the Kafka latestOffset increased by min_delta."""
    start = time.time()
    base = 0
    seen_base = False

    while time.time() - start < timeout_sec:
        progress = query.lastProgress or {}
        total = _sum_latest_offsets_from_progress(progress) if progress else 0
        if not seen_base and progress:
            base = total
            seen_base = True
        if seen_base and (total - base) >= min_delta:
            log.info("[SERVICE_READY][STREAM_OFFSETS] Spark sees Kafka latestOffset grew by %s (total=%s)",
                     total - base, total)
            return
        time.sleep(1.0)
    raise TimeoutError(f"[SERVICE_NOT_READY][STREAM_OFFSETS]Streaming query did not observe Kafka offset growth of {min_delta} within {timeout_sec}s (Δdelta={total - base}, base={base}, last_total={total}).")


def _topic_end_offsets(bootstrap: str, topic: str) -> dict[int, int]:
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
    c.assign(tps)
    for tp in tps:
        c.seek_to_end(tp)
    ends = {tp.partition: c.position(tp) for tp in tps}
    c.close()
    return ends


def wait_for_kafka_increase(
    min_delta: int,
    timeout_sec: int = 60,
    bootstrap: str | None = None,
    topic: str | None = None,
) -> int:
    """
    Wait until Kafka end offsets for `topic` increase by >= min_delta.
    Returns the observed increase; raises TimeoutError on timeout.
    """
    bootstrap = bootstrap or KAFKA_BOOTSTRAP_SERVERS
    topic = topic or KAFKA_TOPIC

    start_offsets = _topic_end_offsets(bootstrap, topic)
    base_total = sum(start_offsets.values())
    start = time.time()
    last_total = base_total

    while time.time() - start < timeout_sec:
        end_offsets = _topic_end_offsets(bootstrap, topic)
        total = sum(end_offsets.values())
        if total - base_total >= min_delta:
            log.info(
                "[SERVICE_READY][KAFKA_OFFSETS] Topic '%s' end-offsets grew by %s (total=%s)",
                topic, total - base_total, total
            )
            return total - base_total
        last_total = total
        time.sleep(1.0)

    raise TimeoutError(
        f"[SERVICE_NOT_READY][KAFKA_OFFSETS] Topic '{topic}' did not grow by {min_delta} "
        f"within {timeout_sec}s (Δ={last_total - base_total}, base={base_total}, last_total={last_total})."
    )
