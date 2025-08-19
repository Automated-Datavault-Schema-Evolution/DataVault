import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit
from logger import log
from config import STAGING_SCHEMA


def _ensure_db(spark: SparkSession):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_SCHEMA}")



def start_bronze_writer(
    spark: SparkSession,
    table_name: str,
    df_stream: DataFrame,
    checkpoint_base: str = "./checkpoints",
):
    """
    Persist the streaming dataframe to a managed Spark table in the staging (bronze) schema.
    Uses foreachBatch to append micro-batches (engine-agnostic). Adds simple audit cols.
    """
    if df_stream is None:
        log.warning("[BRONZE] No stream for %s; skipping writer", table_name)
        return None

    _ensure_db(spark)
    checkpoint_path = os.path.join(checkpoint_base, "bronze", table_name)
    os.makedirs(checkpoint_path, exist_ok=True)

    def write_batch(batch_df: DataFrame, epoch_id: int):
        if batch_df.rdd.isEmpty():
            return
        out_df = (
            batch_df.withColumn("__ingested_at", current_timestamp())
                    .withColumn("__record_source", lit(table_name))
        )
        out_df.write.mode("append").saveAsTable(f"{STAGING_SCHEMA}.{table_name}")

    log.info("[BRONZE] Starting writer for %s -> %s.%s", table_name, STAGING_SCHEMA, table_name)
    query = (
        df_stream.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", checkpoint_path)
        .outputMode("update")
        .start()
    )
    return query


def materialize_bronze_accounts_from_postgres(
    spark: SparkSession,
    *,
    host: str,
    port: int,
    db: str,
    user: str,
    password: str,
    schema,
    source_table,
    target_db,
    target_table,
    mode: str = "overwrite",  # or "append" if you need incremental
):
    """
    Reads the staging RDBMS table and writes it as a Hive managed table bronze.accounts
    in the current metastore (the one dbt uses via Thrift/HMS).
    """
    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"

    # Ensure we can switch schema explicitly (create in bronze db)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_db}")
    spark.sql(f"USE {target_db}")

    # Read from Postgres using Spark JDBC
    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", f"{schema}.{source_table}")
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    # Write to Hive managed table bronze.accounts
    full_table_name = f"{target_db}.{target_table}"
    df.write.mode(mode).saveAsTable(full_table_name)

    # A tiny sanity check in logs
    cnt = spark.table(full_table_name).count()
    log.debug(f"[BRONZE_INGESTOR] Wrote {cnt} row(s) to {full_table_name}")