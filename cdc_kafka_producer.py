import json
import os
import time
from pathlib import Path

import pandas as pd
import psycopg2
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic, NewPartitions
from kafka.errors import TopicAlreadyExistsError
from logger import log
from psycopg2 import sql

from config import (
    LAKE_TYPE, PARQUET_PATH,
    RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER, RDBMS_PASSWORD, RDBMS_SCHEMA,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_PARTITIONS, KAFKA_REPLICATION,
)
from utils.helper_spark import get_spark_session
from utils.performance_logger import PerfListener
from utils.schema_helpers import introspect_lake_columns

WATERMARK_FILE = "/data/state/cdc_watermarks.json"
WATERMARK_DIR = os.path.dirname(WATERMARK_FILE)

from threading import Lock
from pyspark.sql import SparkSession

_SPARK: SparkSession | None = None
_SPARK_LOCK = Lock()
_SPARK_LISTENER_ADDED = False


def _get_spark(app_name: str = "DataVault_CDC_DeltaReader") -> SparkSession:
    """
    Parquet/Delta mode needs Spark to read Delta tables.
    Do NOT call get_spark_session() on every scan; create once and reuse.
    """
    global _SPARK, _SPARK_LISTENER_ADDED

    if _SPARK is None:
        with _SPARK_LOCK:
            if _SPARK is None:
                _SPARK = get_spark_session(app_name)

    # Add listener once (optional, keeps your previous behavior)
    if not _SPARK_LISTENER_ADDED:
        try:
            _SPARK.streams.addListener(PerfListener())
        except Exception:
            pass
        _SPARK_LISTENER_ADDED = True

    return _SPARK


def build_initial_load_sql(table: str) -> str:
    # Use only real columns from the lake
    spark = _get_spark("DataVault_CDC_SchemaIntrospect")
    cols = introspect_lake_columns(spark, table)  # e.g., ['accountid', ...]
    col_list = ", ".join([f'"{c}"' for c in cols])  # quote for safety
    return f'SELECT {col_list} FROM "{RDBMS_SCHEMA}"."{table}"'


# --- Kafka topic management ---
def check_and_create_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=KAFKA_TOPIC,
                           num_partitions=KAFKA_PARTITIONS, replication_factor=KAFKA_REPLICATION, timeout_sec=30):
    """
    Ensures a Kafka topic exists and waits until at least one partition is available.
    """
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    topics = admin.list_topics()
    if topic_name in topics:
        log.debug(f"[Kafka] Topic '{topic_name}' already exists.")
        # ensure it has at least num_partitions
        try:
            prod = KafkaProducer(bootstrap_servers=bootstrap_servers)
            current = prod.partitions_for(topic_name)
            prod.close()
            cur_cnt = len(current) if current else 0
        except Exception as e:
            cur_cnt = 0

        if cur_cnt < num_partitions:
            try:
                log.warning(f"[Kafka] Increasing partitions for '{topic_name}' from {cur_cnt} to {num_partitions}")
                admin.create_partitions({topic_name: NewPartitions(total_count=num_partitions)})
            except Exception as e:
                log.error(f"[Kafka] Could not increase partitions: {e}")

    else:
        log.warning(f"[Kafka] Topic '{topic_name}' does not exist. Creating...")
        topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            admin.create_topics([topic])
            log.info(f"[Kafka] Topic '{topic_name}' created.")
        except TopicAlreadyExistsError:
            log.error(f"[Kafka] Topic '{topic_name}' already created by another process.")
    admin.close()

    # Wait until at least one partition is assigned to the topic
    start = time.time()
    while True:
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            partitions = producer.partitions_for(topic_name)
            producer.close()
            if partitions and len(partitions) > 0:
                log.debug(f"[Kafka] Topic '{topic_name}' is available with {len(partitions)} partition(s).")
                break
            else:
                log.debug(f"[Kafka] Waiting for partitions for topic '{topic_name}'...")
        except Exception as e:
            log.error(f"[Kafka] Waiting for topic '{topic_name}'... ({e})")
        time.sleep(1)
        if time.time() - start > timeout_sec:
            log.critical(f"[Kafka] Timeout: Topic '{topic_name}' does not have partitions after {timeout_sec} seconds.")
            raise TimeoutError(
                f"[Kafka] Timeout: Topic '{topic_name}' does not have partitions after {timeout_sec} seconds.")


# Parquet helpers
def get_parquet_tables():
    """
    In 'parquet' mode, PARQUET_PATH is treated as a Delta Lake root (e.g., /lake):
      /lake/<table>/_delta_log/...
    For backward compatibility we also support legacy '<table>.parquet' files in PARQUET_PATH.
    """
    root = Path(PARQUET_PATH)

    if not root.exists() or not root.is_dir():
        log.warning(f"[CDC Producer] PARQUET_PATH not found or not a directory: {root}")
        return []

    entries = list(root.iterdir())

    # Legacy mode: one file per table
    parquet_files = [p for p in entries if p.is_file() and p.name.endswith(".parquet")]
    if parquet_files:
        return sorted([p.name[:-8] for p in parquet_files])

    # Delta-root mode: directories with _delta_log
    tables = []
    for p in entries:
        if not p.is_dir():
            continue
        name = p.name
        if name.startswith("."):
            continue
        if (p / "_delta_log").is_dir():
            tables.append(name)

    return sorted(tables)


def load_parquet_table(table_name):
    """
    Load a table in 'parquet' mode.
    Supports:
      (A) legacy: PARQUET_PATH/<table>.parquet
      (B) delta-root: PARQUET_PATH/<table>/ (directory containing _delta_log)
    """
    root = Path(PARQUET_PATH)

    legacy_file = root / f"{table_name}.parquet"
    if legacy_file.exists() and legacy_file.is_file():
        return pd.read_parquet(str(legacy_file))

    table_dir = root / str(table_name)
    if table_dir.exists() and table_dir.is_dir() and (table_dir / "_delta_log").is_dir():
        # Delta table directory -> read with Spark so we respect the Delta log.
        try:
            spark = _get_spark("DataVault_CDC_DeltaReader")
            sdf = spark.read.format("delta").load(str(table_dir))
            return sdf.toPandas()
        except Exception as e:
            log.warning(f"[CDC Producer] Failed to read delta table '{table_name}' at {table_dir}: {e}")
            return pd.DataFrame()

    # Fallback: try treating as a parquet dataset directory (pyarrow can read directories)
    if table_dir.exists() and table_dir.is_dir():
        try:
            return pd.read_parquet(str(table_dir))
        except Exception as e:
            log.warning(f"[CDC Producer] Failed to read parquet dataset dir '{table_name}' at {table_dir}: {e}")
            return pd.DataFrame()

    raise FileNotFoundError(f"No parquet/delta table found for '{table_name}' under {root}")



# RDBMS helpers
def get_rdbms_tables():
    conn = psycopg2.connect(
        host=RDBMS_HOST, port=RDBMS_PORT,
        dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = %s",
        (RDBMS_SCHEMA, "BASE TABLE")
    )
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables


def load_rdbms_table(table_name):
    conn = psycopg2.connect(
        host=RDBMS_HOST, port=RDBMS_PORT,
        dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
    )
    cur = conn.cursor()
    query = sql.SQL("SELECT * FROM {}.{}").format(
        sql.Identifier(RDBMS_SCHEMA),
        sql.Identifier(table_name)
    )
    cur.execute(query)
    data = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()
    conn.close()
    df = pd.DataFrame(data, columns=colnames)
    return df


def load_watermarks():
    """Load watermarks from disk, tolerating empty/malformed files."""
    if not os.path.exists(WATERMARK_FILE):
        log.info("No watermark file found; starting fresh")
        return {}
    log.debug(f"Loading watermarks from {WATERMARK_FILE}")
    try:
        with open(WATERMARK_FILE, "r") as f:
            content = f.read().strip()
            if not content:
                log.info(f"Watermark file {WATERMARK_FILE} is empty; starting fresh")
                return {}
            raw = json.loads(content)
    except (OSError, json.JSONDecodeError) as e:
        log.warning(f"Could not parse watermark file {WATERMARK_FILE}: {e}; starting fresh")
        return {}

    parsed = {}
    for tbl, ts in raw.items():
        parsed_ts = pd.to_datetime(ts, errors="coerce", utc=True)
        if pd.isna(parsed_ts):
            log.warning(f"Ignoring invalid watermark for {tbl}: {ts}")
        else:
            parsed[tbl] = parsed_ts
    return parsed


def save_watermarks(wm: dict):
    """Atomically persist watermarks (avoid partial/corrupt files)."""
    os.makedirs(WATERMARK_DIR, exist_ok=True)
    serializable = {}
    for tbl, ts in wm.items():
        if ts is None or pd.isna(ts):
            continue
        # ensure string in ISO 8601 with Z when UTC
        if isinstance(ts, pd.Timestamp):
            if ts.tzinfo is None:
                ts = ts.tz_localize("UTC")
            serializable[tbl] = ts.isoformat()
        else:
            serializable[tbl] = str(ts)

    tmp_path = WATERMARK_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(serializable, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, WATERMARK_FILE)


def produce_tables_once(tables):
    """Produce all rows for the given tables exactly once."""
    check_and_create_topic()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=0,
        acks='all'
    )

    if LAKE_TYPE == "parquet":
        load_func = load_parquet_table
    elif LAKE_TYPE == "rdbms":
        load_func = load_rdbms_table
    else:
        log.critical(f"Unknown LAKE_TYPE '{LAKE_TYPE}' (must be 'parquet' or 'rdbms')")
        raise ValueError("Unknown LAKE_TYPE (must be 'parquet' or 'rdbms')")

    watermarks = load_watermarks()
    produced_counts: dict[str, int] = {}

    for table in tables:
        log.info(f"[CDC Producer] Initial load for {table}")
        try:
            df = load_func(table)
        except Exception as e:
            log.info(f"Error loading {table}: {e}")
            produced_counts[table] = 0
            continue

        if "ingestion_timestamp" not in df.columns:
            log.warning(f"Table {table} skipped: no 'ingestion_timestamp' column for CDC.")
            produced_counts[table] = 0
            continue

        df = df.dropna(subset=["ingestion_timestamp"])
        cnt = 0
        futures = []

        # publish
        for _, row in df.iterrows():
            payload = row.dropna().to_dict()
            ingestion_timestamp = payload.get("ingestion_timestamp")

            # normalize to ISO string
            if isinstance(ingestion_timestamp, pd.Timestamp):
                if ingestion_timestamp.tzinfo is None:
                    ingestion_timestamp = ingestion_timestamp.tz_localize("UTC")
                ingestion_timestamp = ingestion_timestamp.isoformat()

            payload["ingestion_timestamp"] = ingestion_timestamp

            futures.append(producer.send(
                KAFKA_TOPIC,
                {
                    "table": table,
                    "payload": json.dumps(payload, default=str),
                    "cdc_type": "insert",
                    "cdc_ingestion_timestamp": ingestion_timestamp,
                },
            ))
            cnt += 1

        # wait once, after all sends (surface delivery errors)
        for fut in futures:
            fut.get(timeout=30)

        if cnt > 0:
            # df['ingestion_timestamp'] may be mixed types; let pandas compute max then normalize
            max_ts = pd.to_datetime(df["ingestion_timestamp"], errors="coerce", utc=True).max()
            watermarks[table] = max_ts
            log.info(f"[CDC Producer] Produced {cnt} events for {table}. Watermark: {watermarks.get(table)}")
        else:
            log.info(f"[CDC Producer] Produced 0 events for {table}.")

        produced_counts[table] = cnt

    producer.flush()
    save_watermarks(watermarks)
    producer.close()

    return produced_counts


def cdc_producer_insert_only(stop_event=None, skip_full_load_tables=None):
    check_and_create_topic()

    # If a table has no watermark yet, the default behavior is to treat the full table as "new".
    # During service bootstrap we may intentionally postpone full-load for tables that are handled
    # by the dedicated initial-load path (produce_tables_once). This avoids duplicate full loads
    # while still allowing truly new tables (created after startup) to be CDC-produced immediately.
    skip_full_load_tables = set(skip_full_load_tables or [])

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        acks='all'
    )
    log.info(f"[CDC Producer] Insert-only CDC from {LAKE_TYPE.upper()} staging area")
    watermarks = load_watermarks()

    if LAKE_TYPE == "parquet":
        _get_spark("DataVault_CDC_DeltaReader")

    stop = stop_event.is_set if stop_event else (lambda: False)
    while not stop():
        if LAKE_TYPE == "parquet":
            tables = get_parquet_tables()
            load_func = load_parquet_table
        elif LAKE_TYPE == "rdbms":
            tables = get_rdbms_tables()
            load_func = load_rdbms_table
        else:
            log.critical(f"Unknown LAKE_TYPE '{LAKE_TYPE}' (must be 'parquet' or 'rdbms')")
            raise ValueError("Unknown LAKE_TYPE (must be 'parquet' or 'rdbms')")

        for table in tables:
            log.info(f"[CDC Producer] Scanning {table}")
            try:
                df = load_func(table)
            except Exception as e:
                log.info(f"Error loading {table}: {e}")
                continue

            if "ingestion_timestamp" not in df.columns:
                log.warning(f"Table {table} skipped: no 'ingestion_timestamp' column for CDC.")
                continue

            last_ts = watermarks.get(table)
            if last_ts is not None and not pd.isna(last_ts):
                df_ts = pd.to_datetime(df["ingestion_timestamp"], errors="coerce", utc=True)
                new_rows = df[df_ts > last_ts]
            else:
                # No watermark yet:
                # - If this table is part of the startup baseline, let the explicit initial-load path
                #   establish the first watermark.
                # - Otherwise, this is a truly new table: produce full load now.
                if table in skip_full_load_tables:
                    continue
                new_rows = df

            if new_rows.empty:
                continue

            new_rows = new_rows.dropna(subset=["ingestion_timestamp"])
            for _, row in new_rows.iterrows():
                payload = row.dropna().to_dict()
                ingestion_timestamp = payload.get("ingestion_timestamp")
                if isinstance(ingestion_timestamp, pd.Timestamp):
                    if ingestion_timestamp.tzinfo is None:
                        ingestion_timestamp = ingestion_timestamp.tz_localize("UTC")
                    ingestion_timestamp = ingestion_timestamp.isoformat()
                payload["ingestion_timestamp"] = ingestion_timestamp

                producer.send(
                    KAFKA_TOPIC,
                    {
                        "table": table,
                        "payload": json.dumps(payload, default=str),
                        "cdc_type": "insert",
                        "cdc_ingestion_timestamp": ingestion_timestamp
                    }
                )

            max_ts = pd.to_datetime(new_rows["ingestion_timestamp"], errors="coerce", utc=True).max()
            watermarks[table] = max_ts
            log.info(f"[CDC Producer] Produced {len(new_rows)} events for {table}. Watermark: {max_ts}")

        producer.flush()
        save_watermarks(watermarks)
        time.sleep(5)


if __name__ == "__main__":
    cdc_producer_insert_only()
