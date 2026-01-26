import os
import time
from typing import Optional, Dict

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

            if partitions and len(partitions) > 0:
                log.info(f"[SERVICE_READY][KAFKA] Kafka topic '{topic}' has {len(partitions)} partition(s)")
                return
        except Exception as e:
            last_error = e
        time.sleep(1.0)
    raise TimeoutError(f"Kafka not ready for topic '{topic}': {last_error}")


def wait_for_lake(timeout_sec: int = 60):
    """
    Block until the lake is reachable.
    - parquet: treat PARQUET_PATH as a Delta root (/lake). Ready when the directory exists and is listable.
              (Do NOT require '*.parquet' files at root; delta tables are directories.)
    - rdbms: wait until a connection can be established and SELECT 1 succeeds.
    """
    start = time.time()
    last_error = None

    if LAKE_TYPE.lower() == "parquet":
        while time.time() - start < timeout_sec:
            try:
                if os.path.isdir(PARQUET_PATH):
                    # must be listable
                    entries = os.listdir(PARQUET_PATH)
                    # best-effort signal: delta tables are directories containing _delta_log
                    delta_dirs = 0
                    try:
                        for e in entries:
                            p = os.path.join(PARQUET_PATH, e)
                            if os.path.isdir(p) and os.path.isdir(os.path.join(p, "_delta_log")):
                                delta_dirs += 1
                    except Exception:
                        pass
                    log.info(f"[SERVICE_READY][DATA_LAKE] PARQUET_PATH='{PARQUET_PATH}', entries={len(entries)}, delta_tables={delta_dirs}")
                    return
            except Exception as e:
                last_error = e
            time.sleep(1.0)
        raise TimeoutError(f"[SERVICE_NOT_READY][DATA_LAKE] Parquet/Delta lake not ready at '{PARQUET_PATH}': {last_error}")

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
                log.info("[SERVICE_READY][DATA_LAKE] RDBMS connection established")
                return
            except Exception as e:
                last_error = e
                time.sleep(1.0)
        raise TimeoutError(
            f"[SERVICE_NOT_READY][DATA_LAKE] RDBMS lake not ready (host={RDBMS_HOST}, db={RDBMS_DB}): {last_error}"
        )
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


def wait_for_stream_offset_growth(
        query,
        produced_total: int,
        base_total: int | None = None,
        timeout_sec: int = 60,
        poll_interval: float = 1.0,
        nudge: bool = True,
) -> int:
    """
    Wait until the streaming query sees Kafka latest offsets reach:
        target_total = (base_total or 0) + produced_total

    This uses the query's lastProgress["sources"][0]["latestOffset"] counters,
    i.e. an absolute total across partitions, not a delta since the call began.

    Examples:
      - If you measured Kafka baseline at 0 and produced 500, we wait for >= 500.
      - If baseline was 123 and kafka produced 500, we wait for >= 623.

    Returns the final observed total on success. Raises TimeoutError on expiry.
    """
    if produced_total <= 0:
        log.info("[STREAM_OFFSETS] No increase requested (produced_total<=0); nothing to wait for.")
        return 0

    # Helper to sum the total from the query's latest progress snapshot
    def _latest_total(_q) -> int:
        p = getattr(_q, "lastProgress", None) or {}
        return _sum_latest_offsets_from_progress(p) or 0

    target_total = (base_total or 0) + int(produced_total)
    end_ts = time.time() + timeout_sec

    # First snapshot
    last_total = _latest_total(query)

    # Fast-path: already at/over target
    if last_total >= target_total:
        log.info(
            f"[SERVICE_READY][STREAM_OFFSETS] already observed total={last_total} >= target={target_total}; ready"
        )
        return last_total

    # Main wait loop
    while time.time() < end_ts:
        if nudge:
            # Try to help the query make progress quickly
            try:
                query.processAllAvailable()
            except Exception:
                pass

        time.sleep(poll_interval)
        last_total = _latest_total(query)
        log.debug(
            f"[ASSERT][STREAM_OFFSETS][TICK] base={base_total or 0} produced={produced_total} target={target_total} last_total={last_total}")
        if last_total >= target_total:
            log.info(
                f"[SERVICE_READY][STREAM_OFFSETS] Observed total={last_total} >= target={target_total}")
            return last_total

    # Timed out
    raise TimeoutError(
        f"[SERVICE_NOT_READY][STREAM_OFFSETS]"
        f" Streaming query did not reach total>={target_total} within {timeout_sec}s "
        f"(base={base_total or 0}, produced={produced_total}, last_total={last_total})."
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
    raise TimeoutError(
        f"[SERVICE_NOT_READY][KAFKA_OFFSETS] '{topic}' did not reach total {min_total} within {timeout_sec}s")


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
