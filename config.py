import os
from dotenv import load_dotenv

env_type = os.getenv('ENV_TYPE', "local")
load_dotenv(f".env.{env_type}")
load_dotenv(dotenv_path="postgres/db.env")

LAKE_TYPE = os.getenv("LAKE_TYPE", "rdbms")
PARQUET_PATH = os.getenv("PARQUET_PATH", "parquet_files")

# RDBMS settings
RDBMS_HOST = os.getenv("POSTGRES_HOST", "localhost")
RDBMS_PORT = int(os.getenv("POSTGRESS_PORT", 5432))
RDBMS_DB = os.getenv("POSTGRES_DB")
RDBMS_USER = os.getenv("POSTGRES_USER")
RDBMS_PASSWORD = os.getenv("POSTGRES_PASSWORD")
RDBMS_SCHEMA = os.getenv("POSTGRES_SCHEMA", "public")

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "lake_stream")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "earliest")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "datalake-stream")

# Spark
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://datavault-ingestion-spark-master:7077")
# Spark resource tuning parameters
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "2g")
SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "2g")
SPARK_DRIVER_CORES = os.getenv("SPARK_DRIVER_CORES", "1")
SPARK_EXECUTOR_CORES = os.getenv("SPARK_EXECUTOR_CORES", "1")
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
# Autoscaling configuration for Spark workers
SPARK_AUTOSCALE = os.getenv("SPARK_AUTOSCALE", "false").lower() == "true"
SPARK_WORKER_MAX = int(os.getenv("SPARK_WORKER_MAX", "5"))
SPARK_WORKER_MIN = int(os.getenv("SPARK_WORKER_MIN", "1"))
SPARK_WORKER_IMAGE = os.getenv("SPARK_WORKER_IMAGE", "bitnami/spark:latest")
SPARK_WORKER_CONTAINER_PREFIX = os.getenv("SPARK_WORKER_CONTAINER_PREFIX", "spark-worker-")
SPARK_WORKER_CPU_THRESHOLD = float(os.getenv("SPARK_WORKER_CPU_THRESHOLD", "80"))
DOCKER_NETWORK = os.getenv("DOCKER_NETWORK", "data_automation-net")

METADATA_LINEAGE_TYPE = os.getenv("METADATA_LINEAGE_TYPE", "parquet")
METADATA_LINEAGE_PATH = os.getenv("METADATA_LINEAGE_PATH", "meta/lineage.parquet")
PROCESSING_MODE = os.getenv("PROCESSING_MODE", "streaming")

DBT_PROFILES_DIR = os.path.join(os.path.dirname(__file__), "profiles")
