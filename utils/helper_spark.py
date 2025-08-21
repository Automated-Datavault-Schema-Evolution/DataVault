import os
import sys
from pathlib import Path

from logger import log
from pyspark.sql import SparkSession

from config import (
    SPARK_MASTER,
    SPARK_DRIVER_MEMORY,
    SPARK_EXECUTOR_MEMORY,
    SPARK_DRIVER_CORES,
    SPARK_EXECUTOR_CORES,
    SPARK_SQL_SHUFFLE_PARTITIONS,
    SPARK_DYNAMIC_ALLOCATION,
    SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS,
    SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS,
    SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS,
    SPARK_SERIALIZER,
    SPARK_KRYO_BUFFER_MAX,
    SPARK_ADAPTIVE_EXECUTION,
    SPARK_DYNAMIC_SHUFFLE_TRACKING,
    HOST_SPARK_WAREHOUSE_DIR, METASTORE_URI, CONTAINER_SPARK_WAREHOUSE_DIR,
)


import shutil
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

def ensure_spark_warehouse_dir():
    """Ensure Spark's warehouse dir exists and is writable."""
    path = Path(CONTAINER_SPARK_WAREHOUSE_DIR)
    try:
        path.mkdir(parents=True, exist_ok=True)
        # Make sure permissions allow Spark to write
        path.chmod(0o777)
        log.info(f"[Spark Init] Warehouse dir ready at: {path} (perms: {oct(path.stat().st_mode)})")
    except Exception as e:
        log.critical(f"Cannot create warehouse dir {path}: {e}")
        raise RuntimeError(f"Cannot create warehouse dir {path}: {e}")

def get_spark_session(app_name="Kafka_Consumer_Lake_Handler"):
    ensure_spark_warehouse_dir()
    spark_master = SPARK_MASTER
    log.info(f"Initializing Spark session '{app_name}' on master '{spark_master}'")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .enableHiveSupport()
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.cores", SPARK_DRIVER_CORES)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.serializer", SPARK_SERIALIZER)
        .config("spark.kryoserializer.buffer.max", SPARK_KRYO_BUFFER_MAX)
        .config("spark.sql.adaptive.enabled", str(SPARK_ADAPTIVE_EXECUTION).lower())
    )

    if SPARK_DYNAMIC_ALLOCATION:
        builder = (
            builder.config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS)
            .config("spark.dynamicAllocation.maxExecutors", SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS)
            .config("spark.dynamicAllocation.initialExecutors", SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS)
            .config("spark.dynamicAllocation.shuffleTracking.enabled", str(SPARK_DYNAMIC_SHUFFLE_TRACKING).lower())
        )

    # Ensure Spark uses the same Python interpreter for driver and executors
    python_exec = sys.executable
    os.environ.setdefault("PYSPARK_PYTHON", python_exec)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", python_exec)
    builder = builder.config("spark.pyspark.python", python_exec) \
        .config("spark.pyspark.driver.python", python_exec) \
        .config("spark.sql.streaming.kafka.useUninterruptibleThread", "true") \
        .config("spark.sql.warehouse.dir", CONTAINER_SPARK_WAREHOUSE_DIR) \
        .config("spark.hadoop.hive.metastore.uris", METASTORE_URI) \
        .config("spark.sql.catalogImplementation", "hive")
        # .config("hive.metastore.uris", METASTORE_URI) \
    my_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
        "org.apache.kafka:kafka-clients:3.5.1",
        "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
        "org.postgresql:postgresql:42.7.7"
    ]
    builder = builder.config("spark.jars.packages", ",".join(my_packages))
    spark = builder.getOrCreate()
    log.debug(f"Spark configuration: {spark.sparkContext.getConf().getAll()}")
    return spark
