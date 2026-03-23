"""Bronze preflight utilities.

Extracted from the original main.py without behavioral changes.
"""

import os
import time
from typing import Iterable

from logger import log
from config import STAGING_SCHEMA
from utils.schema_helpers import bronze_target_columns
from utils.bronze_ingestor import ensure_bronze_table_exists, ensure_bronze_table_schema

def _wait_bronze_stable_rows(
    spark,
    schema: str,
    table: str,
    *,
    min_rows: int = 1,
    stable_checks: int = 2,
    interval_s: float = 2.0,
    timeout_s: float = 120.0,
) -> int:
    """
    Wait until SELECT COUNT(*) from schema.table is:
      - >= min_rows
      - stable across `stable_checks` consecutive polls

    This prevents dbt raw-vault runs from snapshotting bronze while ingestion is still in progress.
    """
    import time

    deadline = time.time() + float(timeout_s)
    last = None
    stable = 0

    while time.time() < deadline:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {schema}.{table}").collect()[0]["c"]
            cnt = int(cnt)
        except Exception:
            cnt = 0

        if cnt >= min_rows:
            if last is not None and cnt == last:
                stable += 1
            else:
                stable = 0
            last = cnt

            if stable >= (stable_checks - 1):
                return cnt

        time.sleep(float(interval_s))

    return int(last or 0)


def _preflight_bronze_for_tables(tables: Iterable[str]) -> None:
    """
    Ensure bronze.<table> exists and has at least the columns expected from the lake schema.

    This is required for gRPC-triggered dbt runs where schema evolution may occur before
    any CDC payload contains the new column (e.g., email).
    """
    if not tables:
        return

    try:
        # Local import avoids a module-import cycle with helper.dbt_runner.
        from helper.dbt_runner import _get_dbt_preflight_spark
        spark = _get_dbt_preflight_spark()
    except Exception as e:
        log.warning(f'[DVH_CORE][DBT] Spark not available for bronze preflight: {e}')
        return

    from pyspark.sql.types import StructType, StructField, StringType
    import time

    timeout_s = float(os.getenv("DBT_BRONZE_PREFLIGHT_TIMEOUT_S", "120"))
    poll_s = float(os.getenv("DBT_BRONZE_PREFLIGHT_POLL_S", "2"))

    for t in tables:
        tbl = str(t or "").strip().lower()
        if not tbl:
            continue

        # Wait until the lake schema is discoverable (delta table created on first write)
        deadline = time.time() + timeout_s
        cols = []
        logged = False

        while time.time() < deadline:
            cols = bronze_target_columns(spark, tbl) or []
            if cols:
                break

            if not logged:
                log.info(f'[DVH_CORE][DBT] Bronze preflight waiting for lake schema: table={tbl} (timeout_s={timeout_s}, poll_s={poll_s})')
                logged = True

            time.sleep(poll_s)

        if not cols:
            # Do NOT continue into dbt; that will fail with a cryptic TABLE_OR_VIEW_NOT_FOUND.
            raise RuntimeError(
                f"[DBT] Bronze preflight timed out waiting for lake schema for '{tbl}' "
                f"(timeout_s={timeout_s}). Refusing to run dbt because bronze.{tbl} would be missing."
            )

        schema = StructType([StructField(str(c), StringType(), True) for c in cols])

        # Ensure table is registered as external Delta and enforce missing cols.
        ensure_bronze_table_exists(spark, tbl, schema)
        ensure_bronze_table_schema(spark, tbl, schema)

        # Wait until bronze row count stabilizes (prevents empty/partial raw_vault tables).
        try:
            stable_cnt = _wait_bronze_stable_rows(
                spark,
                STAGING_SCHEMA if "STAGING_SCHEMA" in globals() else "bronze",
                tbl,
                min_rows=1,
                stable_checks=3,
                interval_s=2.0,
                timeout_s=180.0,
            )
            log.info(f'[DVH_CORE][DBT] Bronze preflight ready: {tbl} stable_rows={stable_cnt}')
        except Exception as e:
            log.warning(f'[DVH_CORE][DBT] Bronze row-count stability check failed for {tbl}: {e}')

