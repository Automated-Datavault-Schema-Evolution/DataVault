import os
from dotenv import load_dotenv

env_type = os.getenv('ENV_TYPE', "local")
load_dotenv(f".env.{env_type}")


LAKE_TYPE = os.getenv("LAKE_TYPE", "rdbms")
PARQUET_PATH = os.getenv("PARQUET_PATH", "parquet_files")
STAGING_SCHEMA = os.getenv("STAGING_SCHEMA", "bronze")
RAW_VAULT_SCHEMA = os.getenv("RAW_VAULT_SCHEMA", "raw_vault")

# RDBMS settings
RDBMS_HOST = os.getenv("POSTGRES_HOST", "localhost")
RDBMS_PORT = int(os.getenv("POSTGRES_PORT", 5432))
RDBMS_DB = os.getenv("POSTGRES_DB")
RDBMS_USER = os.getenv("POSTGRES_USER")
RDBMS_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RDBMS_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lake_stream")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datalake-stream")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "8"))
KAFKA_REPLICATION = int(os.getenv("KAFKA_REPLICATION", "1"))
STREAM_CHECKPOINT_RESET = os.environ.get("STREAM_CHECKPOINT_RESET", "NO")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")
KAFKA_MAX_OFFSETS_PER_TRIGGER = int(os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "50000"))

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://datavault-ingestion-spark-master:7078")
# Location where Spark stores managed tables; mount this path to persist the vault
HOST_SPARK_WAREHOUSE_DIR = os.getenv("HOST_SPARK_WAREHOUSE_DIR", "./data/spark/warehouse")
CONTAINER_WAREHOUSE_DIR = os.getenv("CONTAINER_WAREHOUSE_DIR", "/data/spark/warehouse")
# Spark resource tuning parameters
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "6g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "6g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "4")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "4")
SPARK_SQL_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SQL_SHUFFLE_PARTITIONS", "200"))
SPARK_DYNAMIC_ALLOCATION = os.getenv("SPARK_DYNAMIC_ALLOCATION", "false").lower() == "true"
# Optional dynamic allocation parameters
SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MIN_EXECUTORS", 1))
SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_MAX_EXECUTORS", 10))
SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS = int(os.getenv("SPARK_DYNAMIC_ALLOCATION_INITIAL_EXECUTORS", 1))
# Additional Spark tuning
SPARK_SERIALIZER = os.getenv("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
SPARK_KRYO_BUFFER_MAX = os.getenv("SPARK_KRYO_BUFFER_MAX", "256m")
SPARK_ADAPTIVE_EXECUTION = os.getenv("SPARK_ADAPTIVE_EXECUTION", "true").lower() == "true"
SPARK_DYNAMIC_SHUFFLE_TRACKING = os.getenv("SPARK_DYNAMIC_SHUFFLE_TRACKING", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS = os.getenv("SPARK_SQL_ADAPTIVE_COALESCE_PARTITIONS", "true").lower() == "true"
SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE = os.getenv("SPARK_SQL_ADAPTIVE_ADVISORY_PARTITION_SIZE", "64m")

# Thift Connection
THRIFT_HOST = os.getenv("THRIFT_HOST", "localhost")
THRIFT_PORT = int(os.getenv("THRIFT_PORT", "10000"))
THRIFT_AUTH = os.getenv("THRIFT_AUTH", "NOSASL")

# Meta store
METADATA_LINEAGE_TYPE = os.getenv("METADATA_LINEAGE_TYPE", "parquet")
METADATA_LINEAGE_PATH = os.getenv("METADATA_LINEAGE_PATH", "meta/lineage.parquet")
METADATA_METADATA_PATH = os.getenv("METADATA_METADATA_PATH", "meta/metadata.parquet")
METASTORE_DB_HOST = os.getenv("METASTORE_DB_HOST", RDBMS_HOST)
METASTORE_DB_PORT = int(os.getenv("METASTORE_DB_PORT", RDBMS_PORT))
METASTORE_DB = os.getenv("METASTORE_DB", 'META_MART')
METASTORE_DB_USER = os.getenv("METASTORE_DB_USER", RDBMS_USER)
METASTORE_DB_PASSWORD = os.getenv("METASTORE_DB_PASSWORD", RDBMS_PASSWORD)
METASTORE_DB_SCHEMA = os.getenv("METASTORE_DB_SCHEMA", 'metastore')
METASTORE_URI = os.getenv("METASTORE_URI", "thrift://hive-metastore:9083")


PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")

# DBT
DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "profiles")
DBT_MODELS_JSON_DIR = os.path.join(os.path.dirname(__file__), "models", "json")
DBT_MODELS_SQL_DIR = os.path.join(os.path.dirname(__file__), "models", "sql")

