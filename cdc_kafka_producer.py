import json
import os
import time

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

WATERMARK_FILE = "cdc_watermarks.json"

def build_initial_load_sql(table: str) -> str:
    # Use only real columns from the lake
    spark = get_spark_session()
    spark.streams.addListener(PerfListener())
    cols = introspect_lake_columns(spark, table)  # e.g., ['accountid', ...]
    col_list = ", ".join([f'"{c}"' for c in cols])  # quote for safety
    return f'SELECT {col_list} FROM "{RDBMS_SCHEMA}"."{table}"'

# --- Kafka topic management ---
def check_and_create_topic(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, topic_name=KAFKA_TOPIC, num_partitions=KAFKA_PARTITIONS, replication_factor=KAFKA_REPLICATION, timeout_sec=30):
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
    return [f[:-8] for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")]


def load_parquet_table(table_name):
    return pd.read_parquet(os.path.join(PARQUET_PATH, table_name + ".parquet"))


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
    # Use psycopg2.sql.Identifier for schema and table names (no static SQL)
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
    if os.path.exists(WATERMARK_FILE):
        log.debug(f"Loading watermarks from {WATERMARK_FILE}")
        try:
            with open(WATERMARK_FILE, "r") as f:
                content = f.read().strip()
                if not content:
                    log.info(f"Watermark file {WATERMARK_FILE} is empty; starting fresh")
                    return {}
                raw = json.loads(content)
                parsed = {}
                for tbl, ts in raw.items():
                    parsed_ts = pd.to_datetime(ts, errors="coerce")
                    if pd.isna(parsed_ts):
                        log.warning(f"Ignoring invalid watermark for {tbl}: {ts}")
                    else:
                        parsed[tbl] = parsed_ts
                return parsed
        except (OSError, json.JSONDecodeError) as e:
            log.warning(f"Could not parse watermark file {WATERMARK_FILE}: {e}; starting fresh")
            return {}
    log.info("No watermark file found; starting fresh")
    return {}


def save_watermarks(wm):
    serializable = {}
    for tbl, ts in wm.items():
        if ts is None or pd.isna(ts):
            continue
        serializable[tbl] = str(ts)
    with open(WATERMARK_FILE, "w") as f:
        json.dump(serializable, f)


def produce_tables_once(tables):
    """Produce all rows for the given tables exactly once."""
    check_and_create_topic()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
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
    for table in tables:
        log.info(f"[CDC Producer] Initial load for {table}")
        try:
            df = load_func(table)
        except Exception as e:
            log.info(f"Error loading {table}: {e}")
            continue
        if "modified_at" not in df.columns:
            log.warning(f"Table {table} skipped: no 'modified_at' column for CDC.")
            continue
        df = df.dropna(subset=["modified_at"])
        for _, row in df.iterrows():
            payload = row.dropna().to_dict()
            modified_at = payload.get("modified_at")
            if isinstance(modified_at, pd.Timestamp):
                modified_at = modified_at.isoformat()
            payload["modified_at"] = modified_at
            producer.send(
                KAFKA_TOPIC,
                {
                    "table": table,
                    "payload": json.dumps(payload, default=str),
                    "cdc_type": "insert",
                    "modified_at": modified_at,
                },
            )
        if not df.empty:
            max_ts = df["modified_at"].max()
            watermarks[table] = max_ts
            log.info(f"[CDC Producer] Produced {len(df)} events for {table}. Watermark: {max_ts}")

    producer.flush()
    save_watermarks(watermarks)
    producer.close()


def cdc_producer_insert_only(stop_event=None):
    # Ensure topic exists and is ready
    check_and_create_topic()

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=100,
        acks='all'
    )
    log.info(f"[CDC Producer] Insert-only CDC from {LAKE_TYPE.upper()} staging area")
    watermarks = load_watermarks()
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
            if "modified_at" not in df.columns:
                log.warning(f"Table {table} skipped: no 'modified_at' column for CDC.")
                continue
            last_ts = watermarks.get(table)
            if last_ts is not None and not pd.isna(last_ts):
                new_rows = df[df["modified_at"] > last_ts]
            else:
                new_rows = df
            if new_rows.empty:
                continue
            new_rows = new_rows.dropna(subset=["modified_at"])
            for _, row in new_rows.iterrows():
                payload = row.dropna().to_dict()
                modified_at = payload.get("modified_at")
                if isinstance(modified_at, pd.Timestamp):
                    modified_at = modified_at.isoformat()
                payload["modified_at"] = modified_at
                producer.send(
                    KAFKA_TOPIC,
                    {
                        "table": table,
                        "payload": json.dumps(payload, default=str),
                        "cdc_type": "insert",
                        "modified_at": modified_at
                    }
                )
            max_ts = new_rows["modified_at"].max()
            watermarks[table] = max_ts
            log.info(f"[CDC Producer] Produced {len(new_rows)} events for {table}. Watermark: {max_ts}")
        producer.flush()
        save_watermarks(watermarks)
        time.sleep(5)


if __name__ == "__main__":
    cdc_producer_insert_only()
