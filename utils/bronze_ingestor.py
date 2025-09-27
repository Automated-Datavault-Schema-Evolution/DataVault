import os
from pathlib import Path
import json

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.errors import AnalysisException

from config import STAGING_SCHEMA, STAGING_BASE_PATH  # ← use staging base path
from utils.schema_helpers import infer_schema_from_cdc_event
from logger import log


def _ensure_db(spark: SparkSession):
    """
    Ensure the staging DB (bronze) exists. Point its LOCATION to STAGING_BASE_PATH so even
    managed tables (if any) won't use /data/spark/warehouse.
    """
    base = Path(STAGING_BASE_PATH).resolve()
    base.mkdir(parents=True, exist_ok=True)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_SCHEMA} LOCATION '{str(base)}'")


def ensure_bronze_table_exists(spark: SparkSession, table_name: str, schema: T.StructType) -> bool:
    """
    Ensure an EXTERNAL Delta table {STAGING_SCHEMA}.{table_name} exists at
    {STAGING_BASE_PATH}/{table_name}.

    - If not present: CREATE TABLE ... USING DELTA LOCATION ...
    - If present but not Delta: try CONVERT TO DELTA (by name or path); else register/convert by path.
    Returns True if created/converted/registered, False if already a Delta table.
    """
    _ensure_db(spark)
    fq = f"{STAGING_SCHEMA}.{table_name}"
    table_dir = Path(STAGING_BASE_PATH).resolve() / table_name
    table_dir.mkdir(parents=True, exist_ok=True)

    def _is_delta_dir(p: Path) -> bool:
        dlog = p / "_delta_log"
        return dlog.is_dir() and any(dlog.iterdir())

    def _has_parquet_files(p: Path) -> bool:
        try:
            for child in p.iterdir():
                if child.is_file() and child.suffix.lower() == ".parquet":
                    return True
            return False
        except FileNotFoundError:
            return False

    def _ddl_for_external(s: T.StructType) -> str:
        cols_sql = ", ".join(f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in s.fields)
        # CREATE TABLE ... USING DELTA LOCATION ... with TBLPROPERTIES (optimize)
        return (
            f"CREATE TABLE IF NOT EXISTS {fq} ({cols_sql}) USING DELTA "
            f"LOCATION '{str(table_dir)}' "
            "TBLPROPERTIES ("
            "  delta.autoOptimize.optimizeWrite = true,"
            "  delta.autoOptimize.autoCompact  = true"
            ")"
        )

    # Fast path: not registered yet → decide based on what's at LOCATION
    if not spark.catalog.tableExists(fq):
        log.info("[BRONZE] Creating external Delta table: %s at %s", fq, table_dir)

        if _is_delta_dir(table_dir):
            spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(table_dir)}'")
            log.info("[BRONZE] Registered existing Delta at %s as %s", table_dir, fq)
            return True

        if _has_parquet_files(table_dir):
            spark.sql(f"CONVERT TO DELTA parquet.`{str(table_dir)}`")
            spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(table_dir)}'")
            log.info("[BRONZE] Converted Parquet at %s to Delta, registered %s", table_dir, fq)
            return True

        # Empty/fresh directory → create brand new empty Delta table with schema
        spark.sql(_ddl_for_external(schema))
        log.info("[BRONZE] Precreated empty table %s", fq)
        return True

    # Table exists: verify format
    try:
        fmt_loc = spark.sql(f"DESCRIBE DETAIL {fq}").select("format", "location").collect()[0]
        fmt = (fmt_loc[0] or "").lower()
        loc = (fmt_loc[1] or "").strip()
    except AnalysisException:
        fmt, loc = "unknown", ""

    if fmt == "delta":
        return False

    log.warning("[BRONZE] Existing table %s is %s; attempting to convert/register", fq, fmt)

    # Try in-place by table name (works for Spark-understood non-delta tables)
    try:
        spark.sql(f"CONVERT TO DELTA {fq}")
        log.info("[BRONZE] Converted %s to Delta in place", fq)
        return True
    except Exception as e:
        log.warning("[BRONZE] In-place CONVERT TO DELTA failed for %s: %s", fq, e)

    # Fallback via path: prefer the table's own location; otherwise external dir
    target_path = Path(loc) if loc else table_dir

    if _is_delta_dir(target_path):
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
        spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(target_path)}'")
        log.info("[BRONZE] Re-registered existing Delta at %s as %s", target_path, fq)
        return True

    if _has_parquet_files(target_path):
        spark.sql(f"CONVERT TO DELTA parquet.`{str(target_path)}`")
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
        spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(target_path)}'")
        log.info("[BRONZE] Converted Parquet at %s to Delta and re-registered %s", target_path, fq)
        return True

    # Last resort: clean registration to external dir when empty/non-existent
    if (not target_path.exists()) or (target_path.exists() and not any(target_path.iterdir())):
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
        target_path.mkdir(parents=True, exist_ok=True)
        cols_sql = ", ".join(f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in schema.fields)
        spark.sql(
            f"CREATE TABLE {fq} ({cols_sql}) USING DELTA LOCATION '{str(target_path)}' "
            "TBLPROPERTIES (delta.autoOptimize.optimizeWrite=true, delta.autoOptimize.autoCompact=true)"
        )
        log.info("[BRONZE] Recreated %s as external Delta at %s", fq, target_path)
        return True

    raise RuntimeError(
        f"[BRONZE] Path {target_path} for {fq} is non-empty and not a Delta/Parquet layout. "
        f"Please clean or move it, then re-run."
    )


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
    spark,
    df_stream,
    *,
    table_name=None,
    table_col="table",
    checkpoint_base=None,
    on_after_write=None,
    allowed_tables=None,
    query_name=None,
    trigger_every=None,
):
    from pyspark import StorageLevel
    from pyspark.sql import DataFrame

    checkpoint_base = checkpoint_base or os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")

    if df_stream is None:
        log.warning("[BRONZE] No stream; skipping writer")
        return None

    def _infer_all_string_schema_from_sample_json(sample_json: str):
        try:
            obj = json.loads(sample_json)
            from pyspark.sql.types import StructType, StructField, StringType

            return StructType([StructField(k, StringType(), True) for k in obj.keys()])
        except Exception as e:
            log.debug("payload schema inference failed: %s", e)
            return None

    def _align_to_existing(target_table: str, df: "DataFrame") -> "DataFrame":
        try:
            if spark.catalog.tableExists(target_table):
                tgt_schema = spark.table(target_table).schema
                tgt_cols = [f.name for f in tgt_schema.fields]
                out = df
                for f in tgt_schema.fields:
                    if f.name not in out.columns:
                        out = out.withColumn(f.name, F.lit(None).cast(f.dataType))
                    else:
                        out = out.withColumn(f.name, F.col(f.name).cast(f.dataType))
                return out.select(*tgt_cols)
        except Exception as e:
            log.debug("[BRONZE] alignment skipped for %s: %s", target_table, e)
        return df

    def _write_one_table(tbl: str, tdf_raw: "DataFrame", batch_id: int) -> int:
        if tdf_raw.rdd.isEmpty():
            log.debug("[BRONZE][%s][batch=%s] empty slice", tbl, batch_id)
            return 0

        sample = tdf_raw.select("payload").where(F.col("payload").isNotNull()).limit(1).collect()
        if not sample:
            log.info("[BRONZE][%s][batch=%s] no sample payload; skipping", tbl, batch_id)
            return 0

        schema = _infer_all_string_schema_from_sample_json(sample[0]["payload"]) or infer_schema_from_cdc_event(
            spark, tbl
        )
        if not schema:
            log.info("[BRONZE][%s][batch=%s] no schema available; skipping", tbl, batch_id)
            return 0

        parsed = (
            tdf_raw.select(F.from_json(F.col("payload"), schema).alias("p"), F.col("cdc_type"), F.col("cdc_modified_at"))
            .select("p.*", "cdc_type", "cdc_modified_at")
            .withColumn("__ingested_at", F.current_timestamp())
            .withColumn("__record_source", F.lit("kafka_cdc"))
        )

        target = f"{STAGING_SCHEMA}.{tbl}"
        out = _align_to_existing(target, parsed).coalesce(8)

        # Always Delta; auto-create on first append (table must have been pre-registered externally)
        (
            out.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )

        written = out.count()
        log.info("[BRONZE][%s][batch=%s] written=%s", tbl, batch_id, written)

        if on_after_write:
            try:
                on_after_write(tbl, batch_id, written)
            except Exception as e:
                log.warning("[BRONZE] on_after_write failed for %s (batch %s): %s", tbl, batch_id, e)
        return written

    # Single-table mode
    if table_name:
        cp = os.path.join(checkpoint_base, STAGING_SCHEMA, table_name)
        os.makedirs(cp, exist_ok=True)

        def _single(batch_df: "DataFrame", batch_id: int):
            bdf = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                _write_one_table(table_name, bdf, batch_id)
            finally:
                bdf.unpersist(blocking=False)

        w = df_stream.writeStream.foreachBatch(_single).option("checkpointLocation", cp).outputMode("append")
        if query_name:
            w = w.queryName(query_name)
        if trigger_every:
            w = w.trigger(processingTime=trigger_every)
        return w.start()

    # Multi-table mode
    from pyspark.sql import functions as F

    if table_col not in df_stream.columns:
        raise ValueError(f"Streaming DF is missing '{table_col}' column for multi-table writes.")

    stream = df_stream
    if allowed_tables:
        stream = stream.where(F.col(table_col).isin(list(allowed_tables)))

    stream = stream.repartition(F.col(table_col))

    cp_dir_name = (query_name or "bronze_multi").replace("/", "_")
    cp = os.path.join(checkpoint_base, cp_dir_name)
    os.makedirs(cp, exist_ok=True)

    def _multi(batch_df: "DataFrame", batch_id: int):
        if batch_df.rdd.isEmpty():
            log.debug("[BRONZE][batch=%s] Empty micro-batch", batch_id)
            return

        required = {"table", "payload", "cdc_type", "cdc_modified_at"}
        missing = required - set(batch_df.columns)
        if missing:
            log.warning("[BRONZE][batch=%s] Missing expected columns: %s", batch_id, sorted(missing))
            return

        from pyspark import StorageLevel

        bdf = (
            batch_df.select("table", "payload", "cdc_type", "cdc_modified_at")
            .where(F.col("table").isNotNull())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        try:
            tables = [r[0] for r in bdf.select("table").distinct().collect()]
            total = 0
            for tbl in tables:
                slice_df = bdf.where(F.col("table") == tbl).persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    total += _write_one_table(tbl, slice_df, batch_id)
                finally:
                    slice_df.unpersist(blocking=False)
            log.info("[BRONZE][batch=%s] tables=%d total_rows=%d", batch_id, len(tables), total)
        finally:
            bdf.unpersist(blocking=False)

    w = stream.writeStream.foreachBatch(_multi).option("checkpointLocation", cp).outputMode("append")
    w = w.queryName(query_name or "bronze_multi")
    if trigger_every:
        w = w.trigger(processingTime=trigger_every)
    return w.start()


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
    # simple fallback for complex types
    return _SQL_TYPE.get(type(dt), "STRING")


def ensure_bronze_table_schema(spark: SparkSession, table_name: str, df_schema: T.StructType):
    fqtn = f"{STAGING_SCHEMA}.{table_name}"

    # Let the first micro-batch create the table with full schema (or pre-create via ensure_bronze_table_exists)
    if not spark.catalog.tableExists(fqtn):
        return

    # Find columns missing in the existing table (case-insensitive)
    have_cols = {f.name.lower() for f in spark.table(fqtn).schema}
    missing = [f for f in df_schema if f.name.lower() not in have_cols]
    if not missing:
        return

    cols_sql = ", ".join(f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in missing)
    spark.sql(f"ALTER TABLE {fqtn} ADD COLUMNS ({cols_sql})")
