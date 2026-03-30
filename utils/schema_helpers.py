"""Schema inspection helpers for bronze and lake reconciliation."""

import json
import os
from typing import List

from domain.table_identity import physical_rdbms_table_name
from logger import log
from pyspark.sql import SparkSession, functions as F

from config import (
    LAKE_TYPE, PARQUET_PATH,
    RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER, RDBMS_PASSWORD, RDBMS_SCHEMA, STAGING_SCHEMA, KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC
)


def _jdbc_url() -> str:
    return f"jdbc:postgresql://{RDBMS_HOST}:{RDBMS_PORT}/{RDBMS_DB}"

def _jdbc_props() -> dict:
    return {"user": RDBMS_USER, "password": RDBMS_PASSWORD, "driver": "org.postgresql.Driver"}

def _escape_sql_literal(s: str) -> str:
    return s.replace("'", "''")


def introspect_lake_columns(spark: SparkSession, table: str) -> List[str]:
    """
    Canonical column list for a lake table (order stable).
    - rdbms: query information_schema.columns case-insensitively (handles quoted mixed-case identifiers)
    - parquet: supports BOTH:
        (A) single-file parquet:   PARQUET_PATH/<table>.parquet
        (B) delta-root layout:     PARQUET_PATH/<table>/  (contains _delta_log/)
    """
    if LAKE_TYPE == "rdbms":
        schema_esc = _escape_sql_literal(RDBMS_SCHEMA)
        logical_table = str(table or "").strip()
        physical_table = physical_rdbms_table_name(logical_table)
        table_esc = _escape_sql_literal(physical_table)

        query = (
            "(SELECT column_name "
            f" FROM information_schema.columns"
            f" WHERE table_schema = '{schema_esc}'"
            f"   AND lower(table_name) = lower('{table_esc}')"
            " ORDER BY ordinal_position) AS cols"
        )

        df = spark.read.jdbc(url=_jdbc_url(), table=query, properties=_jdbc_props())
        cols = [r["column_name"].lower() for r in df.collect()]
        log.debug(
            f"[DVH_UTILS][Schema] RDBMS columns for logical={logical_table} physical={physical_table}: {cols}"
        )
        return cols

    if LAKE_TYPE == "parquet":
        # In this repo "parquet" means *Delta Lake on parquet*.
        # The lake root is PARQUET_PATH (e.g. /lake) with per-table directories.
        # For backwards compatibility, we also support a single-file <table>.parquet layout.

        def _resolve_case_insensitive_entry(root: str, want: str) -> str | None:
            """Resolve an entry under `root` by case-insensitive match."""
            try:
                want_l = want.lower()
                for e in os.listdir(root):
                    if e.lower() == want_l:
                        return os.path.join(root, e)
            except Exception:
                return None
            return None

        def _resolve_delta_dir(root: str, tbl: str) -> str | None:
            # 1) Exact match
            exact = os.path.join(root, tbl)
            if os.path.isdir(os.path.join(exact, "_delta_log")):
                return exact
            # 2) Case-insensitive match
            ci = _resolve_case_insensitive_entry(root, tbl)
            if ci and os.path.isdir(os.path.join(ci, "_delta_log")):
                return ci
            return None

        def _resolve_parquet_file(root: str, tbl: str) -> str | None:
            # 1) Exact match
            exact = os.path.join(root, f"{tbl}.parquet")
            if os.path.exists(exact):
                return exact
            # 2) Case-insensitive match
            ci = _resolve_case_insensitive_entry(root, f"{tbl}.parquet")
            if ci and os.path.exists(ci):
                return ci
            return None

        # 1) Prefer Delta table directory: <PARQUET_PATH>/<table>/
        delta_dir = _resolve_delta_dir(PARQUET_PATH, str(table))
        try:
            if delta_dir:
                df0 = spark.read.format("delta").load(delta_dir).limit(0)
                cols = [c.lower() for c in df0.columns]
                log.debug(f"[DVH_UTILS][Schema] Delta columns for {table}: {cols}")
                return cols
        except Exception as e:
            log.debug(f"[DVH_UTILS][Schema] Delta introspection failed for {table} at {delta_dir}: {e}")

        # 2) Fallback: plain parquet file <PARQUET_PATH>/<table>.parquet
        parquet_file = _resolve_parquet_file(PARQUET_PATH, str(table))
        if not parquet_file or not os.path.exists(parquet_file):
            return []
        df0 = spark.read.parquet(parquet_file).limit(0)
        cols = [c.lower() for c in df0.columns]
        log.debug(f"[DVH_UTILS][Schema] Parquet columns for {table}: {cols}")
        return cols

    raise ValueError(f"Unsupported LAKE_TYPE: {LAKE_TYPE}")



def bronze_target_columns(spark: SparkSession, table: str) -> List[str]:
    """
    Return the authoritative column set for bronze.<table>.

    Critical behavior:
      - ALWAYS consult the lake schema (authoritative) to detect newly evolved columns.
      - Merge with existing bronze schema to avoid dropping columns if bronze has extras.
      - Preserve order: lake order first, then any bronze-only columns appended.
    """
    table = str(table or "").strip()
    if not table:
        return []

    fq = f"{STAGING_SCHEMA}.{table}"

    existing_cols: List[str] = []
    try:
        if spark.catalog.tableExists(fq):
            existing_cols = [f.name for f in spark.table(fq).schema.fields]
    except Exception as e:
        log.debug(f"[DVH_UTILS][Schema] Could not introspect existing {fq}: {e}")

    lake_cols: List[str] = []
    try:
        lake_cols = introspect_lake_columns(spark, table) or []
    except Exception as e:
        log.debug(f"[DVH_UTILS][Schema] Could not introspect lake columns for {table}: {e}")

    merged: List[str] = []
    for c in lake_cols + existing_cols:
        c = str(c).strip().lower()
        if not c or c in ("__ingested_at", "__record_source"):
            continue
        if c not in merged:
            merged.append(c)

    log.info(
        f"[DVH_UTILS][Schema] bronze_target_columns({table}) -> {merged} "
        f"(lake={len(lake_cols)} existing={len(existing_cols)})"
    )
    return merged

def infer_schema_from_cdc_event(spark, table_name: str):
    """
    Fallback schema inference if payload sampling fails.
    IMPORTANT: filter table name case-insensitively.
    """
    from pyspark.sql.functions import col, from_json, lower
    from pyspark.sql.types import StructType, StructField, StringType

    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType()),
    ])

    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )

    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(lower(col("json.table")) == F.lit(str(table_name).lower()))
    sample = df_table.limit(1).collect()
    if not sample:
        return None

    payload_json = json.loads(sample[0]["json"]["payload"])
    fields = [StructField(k, StringType(), True) for k in payload_json.keys()]
    return StructType(fields)
