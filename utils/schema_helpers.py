from typing import List
import os
from pyspark.sql import SparkSession, functions as F
from logger import log
from config import (
    LAKE_TYPE, PARQUET_PATH,
    RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER, RDBMS_PASSWORD, RDBMS_SCHEMA, STAGING_SCHEMA
)

def introspect_lake_columns(spark: SparkSession, table: str) -> List[str]:
    """
    Canonical column list for a lake table (order stable).
    - rdbms: read JDBC schema (0 rows)
    - parquet: read the parquet file schema
    """
    if LAKE_TYPE == "rdbms":
        url = f"jdbc:postgresql://{RDBMS_HOST}:{RDBMS_PORT}/{RDBMS_DB}"
        props = {"user": RDBMS_USER, "password": RDBMS_PASSWORD, "driver": "org.postgresql.Driver"}
        # Limit(0) to fetch just the schema
        df = spark.read.jdbc(url=url, table=f'"{RDBMS_SCHEMA}"."{table}"', properties=props).limit(0)
        cols = [c.lower() for c in df.columns]
        log.debug(f"[Schema] RDBMS columns for {table}: {cols}")
        return cols

    elif LAKE_TYPE == "parquet":
        path = os.path.join(PARQUET_PATH, f"{table}.parquet")
        df = spark.read.parquet(path).limit(0)
        cols = [c.lower() for c in df.columns]
        log.debug(f"[Schema] Parquet columns for {table}: {cols}")
        return cols

    else:
        raise ValueError(f"Unsupported LAKE_TYPE: {LAKE_TYPE}")


def bronze_target_columns(spark: SparkSession, table: str) -> List[str]:
    """
    Target set for <STAGING_SCHEMA>.<table>:
    - If table already exists in the staging schema, use its schema (authoritative).
    - Else use lake schema (introspection).
    NOTE: Does not include the control columns we add (__ingested_at, __record_source).
    """
    try:
        fq = f"{STAGING_SCHEMA}.{table}"
        if spark.catalog.tableExists(fq):  # use public API (no _jsparkSession)
            cols = [f.name for f in spark.table(fq).schema.fields]
            cols = [c.lower() for c in cols if c not in ("__ingested_at", "__record_source")]
            log.debug(f"[Schema] Existing {fq} columns: {cols}")
            return cols
    except Exception as e:
        log.debug(f"[Schema] tableExists/introspection fallback for {STAGING_SCHEMA}.{table}: {e}")
    return introspect_lake_columns(spark, table)


def align_to_columns(df, expected: List[str], keep_extra: bool = False):
    """
    Ensure df has all expected columns (add nulls for missing) and reorder them.
    expected must NOT contain control columns.
    """
    present = {c.lower() for c in df.columns}
    out = df
    for c in expected:
        if c.lower() not in present:
            out = out.withColumn(c, F.lit(None))

    # Reorder: expected first, optionally keep extras
    if keep_extra:
        extras = [x for x in out.columns if x not in expected]
        return out.select(*[F.col(c) for c in expected], *[F.col(x) for x in extras])
    else:
        return out.select(*[F.col(c) for c in expected])
