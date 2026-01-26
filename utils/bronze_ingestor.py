import json
import os
from pathlib import Path

from logger import log
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import types as T

from config import STAGING_SCHEMA, STAGING_BASE_PATH  # staging base path for external delta
from utils.schema_helpers import infer_schema_from_cdc_event, bronze_target_columns


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
    return _SQL_TYPE.get(type(dt), "STRING")


def _ensure_db(spark: SparkSession):
    """
    Ensure the staging DB (bronze) exists. Point its LOCATION to STAGING_BASE_PATH so managed
    objects don't land in spark warehouse unexpectedly.
    """
    base = Path(STAGING_BASE_PATH).resolve()
    base.mkdir(parents=True, exist_ok=True)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {STAGING_SCHEMA} LOCATION '{str(base)}'")


def ensure_bronze_table_schema(spark: SparkSession, table_name: str, df_schema: T.StructType) -> None:
    """
    Ensure the registered bronze Delta table has at least the columns in df_schema.
    Adds missing columns using ALTER TABLE ... ADD COLUMNS.

    Critical for: schema evolves before payload contains the new column.
    """
    _ensure_db(spark)

    table_name = str(table_name or "").strip().lower()
    if not table_name:
        return

    fqtn = f"{STAGING_SCHEMA}.{table_name}"
    if not spark.catalog.tableExists(fqtn):
        return

    try:
        have_cols = {f.name.lower() for f in spark.table(fqtn).schema.fields}
    except Exception as e:
        log.warning("[BRONZE] Could not read existing schema for %s: %s", fqtn, e)
        return

    missing = [f for f in df_schema.fields if f.name.lower() not in have_cols]
    if not missing:
        return

    cols_sql = ", ".join(f"`{f.name}` {_spark_sql_type(f.dataType)}" for f in missing)
    log.info("[BRONZE] Adding missing columns to %s: %s", fqtn, [f.name for f in missing])
    spark.sql(f"ALTER TABLE {fqtn} ADD COLUMNS ({cols_sql})")


def _schema_from_columns(cols: list[str]) -> T.StructType:
    """
    Bronze stores everything as STRING (safe, matches payload parsing).
    """
    fields = [T.StructField(c, T.StringType(), True) for c in cols]
    return T.StructType(fields)


def ensure_bronze_table_matches_lake(spark: SparkSession, table_name: str) -> None:
    """
    Preflight helper (call from gRPC/dbt path):
      - derive the authoritative column set for bronze from lake (or existing bronze)
      - ensure the bronze delta table exists
      - ensure missing columns are added

    This makes dbt runs safe even if no new CDC payload arrives after a schema evolution.
    """
    table_name = str(table_name or "").strip().lower()
    if not table_name:
        return

    _ensure_db(spark)

    try:
        cols = bronze_target_columns(spark, table_name)  # lake schema if table not exists, else bronze schema
    except Exception as e:
        log.warning("[BRONZE] Could not determine lake/bronze target columns for %s: %s", table_name, e)
        cols = []

    if not cols:
        # Nothing to enforce (lake not reachable yet, etc.)
        return

    schema = _schema_from_columns([c.lower() for c in cols])
    ensure_bronze_table_exists(spark, table_name, schema)
    ensure_bronze_table_schema(spark, table_name, schema)


def ensure_bronze_table_exists(spark: SparkSession, table_name: str, schema: T.StructType) -> bool:
    """
    Ensure an EXTERNAL Delta table {STAGING_SCHEMA}.{table_name} exists at
    {STAGING_BASE_PATH}/{table_name}.

    IMPORTANT FIX:
      - If the table already exists as Delta, enforce schema (ADD missing columns).
      - After register/convert, enforce schema as well.
    """
    _ensure_db(spark)

    table_name = str(table_name or "").strip().lower()
    if not table_name:
        raise ValueError("table_name must be a non-empty string")

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
        return (
            f"CREATE TABLE IF NOT EXISTS {fq} ({cols_sql}) USING DELTA "
            f"LOCATION '{str(table_dir)}' "
            "TBLPROPERTIES ("
            "  delta.autoOptimize.optimizeWrite = true,"
            "  delta.autoOptimize.autoCompact  = true"
            ")"
        )

    # Not registered yet → decide based on LOCATION content
    if not spark.catalog.tableExists(fq):
        log.info("[BRONZE] Creating external Delta table: %s at %s", fq, table_dir)

        if _is_delta_dir(table_dir):
            spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(table_dir)}'")
            log.info("[BRONZE] Registered existing Delta at %s as %s", table_dir, fq)
            ensure_bronze_table_schema(spark, table_name, schema)
            return True

        if _has_parquet_files(table_dir):
            spark.sql(f"CONVERT TO DELTA parquet.`{str(table_dir)}`")
            spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(table_dir)}'")
            log.info("[BRONZE] Converted Parquet at %s to Delta, registered %s", table_dir, fq)
            ensure_bronze_table_schema(spark, table_name, schema)
            return True

        # Empty directory → create brand new Delta table with schema
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
        # FIX: enforce schema even when already delta
        ensure_bronze_table_schema(spark, table_name, schema)
        return False

    log.warning("[BRONZE] Existing table %s is %s; attempting to convert/register", fq, fmt)

    # Try in-place by table name
    try:
        spark.sql(f"CONVERT TO DELTA {fq}")
        log.info("[BRONZE] Converted %s to Delta in place", fq)
        ensure_bronze_table_schema(spark, table_name, schema)
        return True
    except Exception as e:
        log.warning("[BRONZE] In-place CONVERT TO DELTA failed for %s: %s", fq, e)

    # Fallback via path
    target_path = Path(loc) if loc else table_dir

    if _is_delta_dir(target_path):
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
        spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(target_path)}'")
        log.info("[BRONZE] Re-registered existing Delta at %s as %s", target_path, fq)
        ensure_bronze_table_schema(spark, table_name, schema)
        return True

    if _has_parquet_files(target_path):
        spark.sql(f"CONVERT TO DELTA parquet.`{str(target_path)}`")
        spark.sql(f"DROP TABLE IF EXISTS {fq}")
        spark.sql(f"CREATE TABLE {fq} USING DELTA LOCATION '{str(target_path)}'")
        log.info("[BRONZE] Converted Parquet at %s to Delta and re-registered %s", target_path, fq)
        ensure_bronze_table_schema(spark, table_name, schema)
        return True

    # Last resort: recreate when empty/non-existent
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
    Truncate bronze after ingestion — only if the table exists.
    """
    _ensure_db(spark)

    table_name = str(table_name or "").strip().lower()
    if not table_name:
        return

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
    from pyspark.sql import functions as F

    checkpoint_base = checkpoint_base or os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")

    if df_stream is None:
        log.warning("[BRONZE] No stream; skipping writer")
        return None
    _ensure_db(spark)

    def _infer_payload_keys(sample_json: str) -> list[str]:
        try:
            obj = json.loads(sample_json)
            return [str(k).lower() for k in obj.keys()]
        except Exception:
            return []

    def _align_to_existing(target_table: str, df: "DataFrame") -> "DataFrame":
        try:
            if spark.catalog.tableExists(target_table):
                tgt_schema = spark.table(target_table).schema
                tgt_cols = [f.name for f in tgt_schema.fields]
                present = {c.lower(): c for c in df.columns}
                out = df
                for f in tgt_schema.fields:
                    src_col = present.get(f.name.lower())
                    if src_col is None:
                        out = out.withColumn(f.name, F.lit(None).cast(f.dataType))
                    else:
                        out = out.withColumn(f.name, F.col(src_col).cast(f.dataType))
                        present[f.name.lower()] = f.name
                return out.select(*tgt_cols)
        except Exception as e:
            log.debug("[BRONZE] alignment skipped for %s: %s", target_table, e)
        return df

    def _write_one_table(tbl: str, tdf_raw: "DataFrame", batch_id: int) -> int:
        tbl = str(tbl or "").strip().lower()
        if not tbl:
            log.info("[BRONZE][batch=%s] empty table name; skipping", batch_id)
            return 0
        if tdf_raw.rdd.isEmpty():
            log.debug("[BRONZE][%s][batch=%s] empty slice", tbl, batch_id)
            return 0

        sample = tdf_raw.select("payload").where(F.col("payload").isNotNull()).limit(1).collect()
        if not sample:
            log.info("[BRONZE][%s][batch=%s] no sample payload; skipping", tbl, batch_id)
            return 0

        payload_cols = _infer_payload_keys(sample[0]["payload"])

        # IMPORTANT: include lake columns (via bronze_target_columns → lake schema if needed)
        lake_cols = []
        try:
            lake_cols = [c.lower() for c in bronze_target_columns(spark, tbl)]
        except Exception as e:
            log.debug("[BRONZE][%s] bronze_target_columns failed: %s", tbl, e)

        merged_cols = []
        for c in lake_cols + payload_cols:
            if c and c not in merged_cols:
                merged_cols.append(c)

        schema = _schema_from_columns(merged_cols)

        # If we couldn't determine anything, fall back to CDC event inference
        if not merged_cols:
            schema = infer_schema_from_cdc_event(spark, tbl)
            if not schema:
                log.info("[BRONZE][%s][batch=%s] no schema available; skipping", tbl, batch_id)
                return 0

        # Ensure table exists and schema is enforced
        ensure_bronze_table_exists(spark, tbl, schema)
        ensure_bronze_table_schema(spark, tbl, schema)

        parsed = (
            tdf_raw.select(
                F.from_json(F.col("payload"), schema).alias("p"),
                F.col("cdc_type"),
                F.col("cdc_modified_at"),
            )
            .select("p.*", "cdc_type", "cdc_modified_at")
            .withColumn("__ingested_at", F.current_timestamp())
            .withColumn("__record_source", F.lit("kafka_cdc"))
        )

        target = f"{STAGING_SCHEMA}.{tbl}"
        out = _align_to_existing(target, parsed).coalesce(8)

        (
            out.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(target)
        )

        # written = out.count()
        written = tdf_raw.count()
        log.info("[BRONZE][%s][batch=%s] written=%s", tbl, batch_id, written)
        return written

    # Single-table mode
    if table_name:
        table_name = str(table_name).strip().lower()
        cp = os.path.join(checkpoint_base, STAGING_SCHEMA, table_name)
        os.makedirs(cp, exist_ok=True)

        def _single(batch_df: "DataFrame", batch_id: int):
            bdf = batch_df.persist(StorageLevel.MEMORY_AND_DISK)
            try:
                written = _write_one_table(table_name, bdf, batch_id)
                if on_after_write:
                    try:
                        touched = [table_name] if written > 0 else []
                        on_after_write(touched, batch_id, {table_name: written} if written > 0 else {})
                    except Exception as e:
                        log.warning("[BRONZE] on_after_write failed for batch %s: %s", batch_id, e)
            finally:
                bdf.unpersist(blocking=False)

        w = df_stream.writeStream.foreachBatch(_single).option("checkpointLocation", cp).outputMode("append")
        if query_name:
            w = w.queryName(query_name)
        if trigger_every:
            w = w.trigger(processingTime=trigger_every)
        return w.start()

    # Multi-table mode
    if table_col not in df_stream.columns:
        raise ValueError(f"Streaming DF is missing '{table_col}' column for multi-table writes.")

    stream = df_stream
    if allowed_tables:
        allowed_norm = [str(t).strip().lower() for t in allowed_tables]
        stream = stream.where(F.lower(F.col(table_col)).isin(allowed_norm))

    stream = stream.repartition(F.lower(F.col(table_col)))

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

        bdf = (
            batch_df.select(
                F.lower(F.col("table")).alias("table"),
                "payload",
                "cdc_type",
                "cdc_modified_at",
            )
            .where(F.col("table").isNotNull())
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        touched = []
        per_table_counts = {}
        total = 0

        try:
            tables = [r[0] for r in bdf.select("table").distinct().collect()]
            for tbl in tables:
                slice_df = bdf.where(F.col("table") == tbl).persist(StorageLevel.MEMORY_AND_DISK)
                try:
                    written = _write_one_table(tbl, slice_df, batch_id)
                    if written > 0:
                        touched.append(tbl)
                        per_table_counts[tbl] = written
                        total += written
                finally:
                    slice_df.unpersist(blocking=False)

            log.info("[BRONZE][batch=%s] tables=%d total_rows=%d", batch_id, len(tables), total)

            if on_after_write:
                try:
                    on_after_write(touched, batch_id, per_table_counts)
                except Exception as e:
                    log.warning("[BRONZE] on_after_write failed for batch %s: %s", batch_id, e)
        finally:
            bdf.unpersist(blocking=False)

    w = stream.writeStream.foreachBatch(_multi).option("checkpointLocation", cp).outputMode("append")
    w = w.queryName(query_name or "bronze_multi")
    if trigger_every:
        w = w.trigger(processingTime=trigger_every)
    return w.start()
