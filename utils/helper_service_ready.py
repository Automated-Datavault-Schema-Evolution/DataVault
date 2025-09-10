import time
import os

import psycopg2
from logger import log

from config import LAKE_TYPE, PARQUET_PATH, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER, RDBMS_PASSWORD


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