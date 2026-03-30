import os
from dotenv import load_dotenv

# Load env files (non-overriding) to support local + docker runs consistently.
env_type = os.getenv("ENV_TYPE", "local")
load_dotenv(f".env.{env_type}", override=False)
load_dotenv(".env", override=False)

# ---- Lake configuration ----
LAKE_TYPE = (os.getenv("LAKE_TYPE") or os.getenv("E2E_LAKE_TYPE") or "rdbms").strip().lower()

LAKE_PATH = os.getenv("LAKE_PATH", "/lake")
DELTA_PATH = os.getenv("DELTA_PATH", LAKE_PATH)

# PARQUET_PATH is historically used by the CDC producer. In "parquet" mode this is actually a Delta root (/lake).
# Make it robust even if compose forgot to set PARQUET_PATH.
PARQUET_PATH = os.getenv("PARQUET_PATH") or os.getenv("DELTA_PATH") or os.getenv("LAKE_PATH") or "parquet_files"

# ---- Vault / staging ----
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "bronze")
RAW_VAULT_SCHEMA = os.getenv("RAW_VAULT_SCHEMA", "raw_vault")

# Support both env var names used across stacks.
STAGING_BASE_PATH = os.getenv("STAGING_BASE_PATH") or os.getenv("BRONZE_BASE_PATH") or "/data/bronze"
RAW_VAULT_BASE_PATH = os.getenv("RAW_VAULT_BASE_PATH", "/data/raw_vault")

# Python executor paths
DRIVER_PY = os.getenv("DRIVER_PY", "/usr/local/bin/python")
EXEC_PY = os.getenv("EXEC_PY", "/opt/bitnami/python/bin/python")
SPARK_IVY_PATH = os.getenv("SPARK_IVY_PATH", "/tmp/.ivy2")

# ---- RDBMS settings ----
RDBMS_HOST = os.getenv("POSTGRES_HOST", "localhost")
RDBMS_PORT = int(os.getenv("POSTGRES_PORT", 5432))
RDBMS_DB = os.getenv("POSTGRES_DB")
RDBMS_USER = os.getenv("POSTGRES_USER")
RDBMS_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RDBMS_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")
POSTGRES_POOL_MIN = int(os.getenv("POSTGRES_POOL_MIN", "1"))
POSTGRES_POOL_MAX = int(os.getenv("POSTGRES_POOL_MAX", "30"))

# ---- Kafka ----
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lake_stream")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datalake-stream")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "8"))
KAFKA_REPLICATION = int(os.getenv("KAFKA_REPLICATION", "1"))
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/data/checkpoints")
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000"))

# ---- Spark ----
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://datavault-ingestion-spark-master:7078")
HOST_SPARK_WAREHOUSE_DIR = os.getenv("HOST_SPARK_WAREHOUSE_DIR", "./data/spark/warehouse")
CONTAINER_WAREHOUSE_DIR = os.getenv("CONTAINER_WAREHOUSE_DIR", "/data/spark/warehouse")

SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "4")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "4")
SPARK_SQL_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS", 1))
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS", 10))
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS", 1))
SPARK_SERIALIZER = os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
SPARK_KRYO_BUFFER_MAX = os.getenv("SPARK_KRYO_BUFFER_MAX", "256m")
SPARK_ADAPTIVE_EXECUTION = os.getenv("SPARK_ADAPTIVE_EXECUTION", "true").lower() == "true"
SPARK_DYNAMIC_SHUFFLE_TRACKING = os.getenv("SPARK_DYNAMIC_SHUFFLE_TRACKING", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE = os.getenv("SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE", "64m")

# Thrift
THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))
THRIFT_AUTH = os.getenv("THRIFT_AUTH", "NOSASL")

# Metastore
METADATA_LINEAGE_TYPE = os.getenv("METADATA_LINEAGE_TYPE", "parquet")
METADATA_LINEAGE_PATH = os.getenv("METADATA_LINEAGE_PATH", "meta/lineage.parquet")
METADATA_METADATA_PATH = os.getenv("METADATA_METADATA_PATH", "meta/metadata.parquet")
METASTORE_DB_HOST = os.getenv("METASTORE_DB_HOST", RDBMS_HOST)
METASTORE_DB_PORT = int(os.getenv("METASTORE_DB_PORT", RDBMS_PORT))
METASTORE_DB = os.getenv("METASTORE_DB", "META_MART")
METASTORE_DB_USER = os.getenv("METASTORE_DB_USER", RDBMS_USER)
METASTORE_DB_PASSWORD = os.getenv("METASTORE_DB_PASSWORD", RDBMS_PASSWORD)
METASTORE_DB_SCHEMA = os.getenv("METASTORE_DB_SCHEMA", "metastore")
METASTORE_URI = os.getenv("METASTORE_URI", "thrift://hive-metastore:9083")

# DBT
DBT_DEBOUNCE_SECONDS = int(os.getenv("DBT_DEBOUNCE_SECONDS", "60"))
DBT_MAX_MODELS_PER_RUN = int(os.getenv("DBT_MAX_MODELS_PER_RUN", "999999"))
STREAM_TRIGGER = os.getenv("STREAM_TRIGGER", "2 seconds")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")

DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "profiles")
DBT_MODELS_JSON_DIR = os.path.join(os.path.dirname(__file__), "models", "json")
DBT_MODELS_SQL_DIR = os.path.join(os.path.dirname(__file__), "models", "sql")

# Maintenance / prune
CONTROL_DIR = os.getenv("CONTROL_DIR", "/app/control")
MAINTENANCE_FLAG = os.path.join(CONTROL_DIR, "PRUNE_REQUEST")
MAINTENANCE_STATUS = os.path.join(CONTROL_DIR, "PRUNE_STATUS")
RETENTION_DAYS = int(os.getenv("BRONZE_RETENTION_DAYS", "30"))
VACUUM_RETAIN_HOURS = int(os.getenv("VACUUM_RETAIN_HOURS", "168"))

BRONZE_SCHEMA = os.getenv("BRONZE_SCHEMA", "bronze")
