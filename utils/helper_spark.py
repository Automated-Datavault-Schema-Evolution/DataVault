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
    METASTORE_URI,
    CONTAINER_WAREHOUSE_DIR,
    HOST_SPARK_WAREHOUSE_DIR, SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS, SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE,
    DRIVER_PY,
    EXEC_PY, SPARK_IVY_PATH,
)


# TODO: REFACTOR!! --> EITHER DELETE AND ONLY RUN IN DOCKER, OR CHANGE LOGIC FOR DYNAMIC PATHS IN env-file
def _pick_warehouse_dir() -> Path:
    is_local = str(SPARK_MASTER).startswith("local") or os.getenv("ENV_TYPE", "local") == "local"
    base = HOST_SPARK_WAREHOUSE_DIR if is_local else CONTAINER_WAREHOUSE_DIR
    return Path(base).resolve()

def ensure_spark_warehouse_dir():
    """Ensure Spark's warehouse dir exists and is writable."""
    path = _pick_warehouse_dir()
    # Best-effort perms; do NOT fail if EPERM on bind mounts
    try:
        path.chmod(0o775)
    except PermissionError as e:
        log.warning(f"Cannot chmod warehouse dir {path}: {e}")

    # Verify we can actually write here; if not, that's fatal
    testfile = path / ".perm_test"
    try:
        with open(testfile, "w") as f:
            f.write("ok")
        testfile.unlink(missing_ok=True)
    except Exception as e:
        log.critical(f"Warehouse {path} is not writable by UID {os.getuid()}: {e}")
        raise


def get_spark_session(app_name="Kafka_Consumer_Lake_Handler"):
    ensure_spark_warehouse_dir()
    warehouse_dir = str(_pick_warehouse_dir())
    spark_master = SPARK_MASTER
    log.info(f"Initializing Spark session '{app_name}' on master '{spark_master}'")
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .enableHiveSupport()
        # Python envs
        .config("spark.pyspark.driver.python", DRIVER_PY)
        .config("spark.pyspark.python", EXEC_PY)
        .config("spark.executorEnv.PYSPARK_PYTHON", EXEC_PY)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.jars.ivy", SPARK_IVY_PATH)
        .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
        .config("spark.executor.extraJavaOptions", "-Duser.home=/tmp")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.driver.cores", SPARK_DRIVER_CORES)
        .config("spark.executor.cores", SPARK_EXECUTOR_CORES)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .config("spark.serializer", SPARK_SERIALIZER)
        .config("spark.kryoserializer.buffer.max", SPARK_KRYO_BUFFER_MAX)
        .config("spark.sql.adaptive.enabled", str(SPARK_ADAPTIVE_EXECUTION).lower())
        .config("spark.sql.adaptive.coalescePartitions.enabled", str(SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS).lower())
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE)
        .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
        # Hive + warehouse
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.hadoop.hive.metastore.uris", METASTORE_URI)
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.legacy.allowCreatingManagedTableUsingExistingLocation", "true")
        # Delta (JARs shipped directly)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.sources.default", "delta")
        # Streaming stability
        .config("spark.sql.streaming.kafka.useUninterruptibleThread", "true")
        # application ui
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4042")
    )

    if SPARK_DYNAMIC_ALLOCATION:
        builder = (
            builder.config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.shuffleTracking.enabled", str(SPARK_DYNAMIC_SHUFFLE_TRACKING).lower())
            .config("spark.dynamicAllocation.minExecutors", SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS)
            .config("spark.dynamicAllocation.maxExecutors", SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS)
            .config("spark.dynamicAllocation.initialExecutors", SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS)
        )

    # --- Ivy + user.home (mirrors the working minimal setup) ---
    builder = (
        builder
        .config("spark.jars.ivy", SPARK_IVY_PATH or "/tmp/.ivy2")
        .config("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
        .config("spark.executor.extraJavaOptions", "-Duser.home=/tmp")
    )

    # --- Delta + Kafka jars (mounted, offline-friendly) ---
    requested_jars = [
        "/opt/delta-jars/delta-spark_2.12-3.3.0.jar",
        "/opt/delta-jars/delta-storage-3.3.0.jar",
        "/opt/ext-jars/spark-sql-kafka-0-10_2.12-3.5.6.jar",
        "/opt/ext-jars/spark-token-provider-kafka-0-10_2.12-3.5.6.jar",
        "/opt/ext-jars/kafka-clients-3.5.1.jar",
        "/opt/jdbc-jars/postgresql-42.7.4.jar",
        "/opt/ext-jars/commons-pool2-2.12.1.jar"
    ]
    existing_jars = [p for p in requested_jars if os.path.exists(p)]
    for p in requested_jars:
        log.info(f"[JAR_CHECK] {p} exists={os.path.exists(p)}")
    if existing_jars:
        builder = builder.config("spark.jars", ",".join(existing_jars))
    else:
        log.warning("[JAR_CHECK] None of the expected jars were found under /opt/(delta-jars|ext-jars). "
                    "Spark may try Ivy (Maven) if ALLOW_MAVEN=1.")

    # Optional online fallback if ALLOW_MAVEN=1
    if os.getenv("ALLOW_MAVEN") == "1":
        builder = builder.config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6",
                "org.apache.kafka:kafka-clients:3.5.1",
                "org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.6",
                "org.postgresql:postgresql:42.7.4",
                "org.apache.commons:commons:2.12.1"
            ])
        )
    spark = builder.getOrCreate()
    log.debug(f"Spark configuration: {spark.sparkContext.getConf().getAll()}")
    return spark


def get_active_stream_query_by_name(name: str, spark=None, wait_for: float = 0.0):
    """
    Return the active Structured Streaming query with the given name, or None.

    Args:
      name: The StreamingQuery.name you set via writeStream.queryName(...)
      spark: Optional SparkSession. If None, resolves the active session (or creates one).
      wait_for: Seconds to poll for the query to appear (0 = no waiting).

    Returns:
      pyspark.sql.streaming.StreamingQuery or None
    """
    import time
    try:
        from pyspark.sql import SparkSession
    except Exception:
        return None  # pyspark not available

    # Resolve a SparkSession
    if spark is None:
        spark = SparkSession.getActiveSession()
        if spark is None:
            try:
                spark = get_spark_session("DataVault_Bootstrap")
            except NameError:
                spark = SparkSession.builder.getOrCreate()

    deadline = time.time() + float(wait_for or 0.0)
    while True:
        try:
            for q in spark.streams.active:
                if getattr(q, "name", None) == name:
                    return q
        except Exception as e:
            try:
                log.debug("While scanning active streams for '%s': %s", name, e)
            except Exception:
                pass

        if time.time() >= deadline:
            # Optional: one-time debug of what *was* active
            try:
                active_names = [getattr(q, "name", "<unnamed>") for q in spark.streams.active]
                log.debug("No stream named '%s' found; active=%s", name, active_names)
            except Exception:
                pass
            return None

        time.sleep(0.5)