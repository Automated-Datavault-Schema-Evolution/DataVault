import os

from logger import log
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql import functions as F
from pyspark.sql import types as T

from config import STAGING_SCHEMA
from utils.schema_helpers import align_to_columns, bronze_target_columns


def _ensure_db(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_SCHEMA}")


def ensure_bronze_table_exists(spark: SparkSession, table_name: str, schema) -> bool:
    """
    Create an EMPTY managed table bronze.<table_name> with the provided schema if it doesn't exist.
    Returns True if created, False if it already existed.
    """
    _ensure_db(spark)
    fq = f"{STAGING_SCHEMA}.{table_name}"
    if spark.catalog.tableExists(fq):
        return False
    empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
    # 'overwrite' to guarantee creation; table does not exist yet
    empty_df.write.mode("overwrite").saveAsTable(fq)
    log.info("[BRONZE] Precreated empty table %s", fq)
    return True


def truncate_bronze_table(spark: SparkSession, table_name: str) -> None:
    """
    Always truncate bronze after ingestion — but only if the table exists.
    (dbt might run before the first micro-batch; don't error out.)
    """
    _ensure_db(spark)
    fq = f"{STAGING_SCHEMA}.{table_name}"
    if not spark.catalog.tableExists(fq):
        log.info("[BRONZE] Table %s not found; skipping truncate", fq)
        return
    spark.sql(f"TRUNCATE TABLE {fq}")
    log.info("[BRONZE] Truncated %s", fq)


def start_bronze_writer(
        spark: SparkSession,
        table_name: str,
        df_stream: DataFrame,
        checkpoint_base: str = "./data/checkpoints",
        on_after_write=None
):
    """
    Persist the streaming dataframe to a managed Spark table in the bronze schema.
    Uses foreachBatch to append micro-batches (engine-agnostic). Adds simple audit cols.
    """
    if df_stream is None:
        log.warning("[BRONZE] No stream for %s; skipping writer", table_name)
        return None
    _ensure_db(spark)
    # Ensure the target table exists BEFORE any dbt run that references it.
    try:
        ensure_bronze_table_exists(spark, table_name, df_stream.schema)
    except Exception as e:
        log.warning("[BRONZE] Could not precreate %s.%s: %s", STAGING_SCHEMA, table_name, e)
    checkpoint_path = os.path.join(checkpoint_base, STAGING_SCHEMA, table_name)
    os.makedirs(checkpoint_path, exist_ok=True)

    def write_batch(batch_df: DataFrame, batch_id: int):
        # Fast empty-batch check
        if batch_df.limit(1).count() == 0:
            log.info("[BRONZE][%s][batch=%s] Empty micro-batch, nothing to write", table_name, batch_id)
            return

        # 1) Align to expected business columns (no control cols yet)
        expected = bronze_target_columns(spark, table_name)  # returns only data/business columns
        out_df = align_to_columns(batch_df, expected)  # keep_extra defaults to False

        in_cnt = batch_df.count()

        # 2) Add control columns AFTER alignment
        out_df = (out_df
                  .withColumn("__ingested_at", F.current_timestamp())
                  .withColumn("__record_source", F.lit("kafka_cdc")))

        # 3) Final order (business + control at the end)
        out_df = out_df.select(*expected, "__ingested_at", "__record_source")

        # 4) Ensure table can accept this schema (adds missing columns if needed)
        ensure_bronze_table_schema(spark, table_name, out_df.schema)

        # 5) Append
        out_cnt = out_df.count()
        out_df.write.mode("append").saveAsTable(f"{STAGING_SCHEMA}.{table_name}")

        total_cnt = spark.table(f"{STAGING_SCHEMA}.{table_name}").count()
        log.info(
            "[BRONZE][%s][batch=%s] incoming=%s, written=%s, bronze_total=%s",
            table_name, batch_id, in_cnt, out_cnt, total_cnt
        )

        # Optional callback
        if on_after_write is not None:
            try:
                on_after_write(batch_id)
            except Exception as e:
                log.warning("[BRONZE] on_after_write failed for %s (batch %s): %s", table_name, batch_id, e)


    log.info("[BRONZE] Starting writer for %s -> %s.%s", table_name, STAGING_SCHEMA, table_name)
    query = (
        df_stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
    )
    return query

_SQL_TYPE = {
    T.StringType: "STRING",
    T.IntegerType: "INT",
    T.LongType: "BIGINT",
    T.DoubleType: "DOUBLE",
    T.FloatType: "FLOAT",
    T.BooleanType: "BOOLEAN",
    T.TimestampType: "TIMESTAMP",
    T.DateType: "DATE",
}

def _spark_sql_type(dt: T.DataType) -> str:
    return _SQL_TYPE.get(type(dt), "STRING")  # simple fallback for complex types

def ensure_bronze_table_schema(spark, table_name: str, df_schema: T.StructType):
    fqtn = f"{STAGING_SCHEMA}.{table_name}"

    # let first micro-batch create the table with full schema
    if not spark._jsparkSession.catalog().tableExists(STAGING_SCHEMA, table_name):
        return

    # find columns missing in the existing table
    have_cols = {f.name.lower() for f in spark.table(fqtn).schema}
    missing = [f for f in df_schema if f.name.lower() not in have_cols]
    if not missing:
        return

    cols_sql = ", ".join(f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in missing)
    spark.sql(f"ALTER TABLE {fqtn} ADD COLUMNS ({cols_sql})")