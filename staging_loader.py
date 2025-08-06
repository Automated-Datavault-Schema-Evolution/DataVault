import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType

from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID,
    SPARK_MASTER, SPARK_DRIVER_MEMORY, SPARK_EXECUTOR_MEMORY, SPARK_SQL_SHUFFLE_PARTITIONS, SPARK_ADAPTIVE_EXECUTION
)

def get_spark():
    spark = (
        SparkSession.builder
        .master(SPARK_MASTER)
        .appName("dv_automation")
        .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
        .config("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.shuffle.partitions", SPARK_SQL_SHUFFLE_PARTITIONS)
        .getOrCreate()
    )
    spark.conf.set("spark.sql.adaptive.enabled", SPARK_ADAPTIVE_EXECUTION)
    return spark

def get_kafka_stream(spark, table_name, schema):
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("groupIdPrefix", KAFKA_GROUP_ID)
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    df_data = df_table.select(from_json(col("json.payload"), schema).alias("data")).select("data.*")
    return df_data
