"""Lake discovery functions.

discover_lake() behavior is unchanged from the original main.py.
"""

from pathlib import Path
import pandas as pd

from logger import log
from config import LAKE_TYPE, PARQUET_PATH, RDBMS_SCHEMA
from helper.postgres_helper import connect_postgres, release_postgres_connection
from helper.parquet_helper import is_delta_table_dir, schema_from_delta_log, fallback_schema_from_parquet


def discover_lake():
    """
    Discover lake tables and return:
      - tables: List[str]
      - load_table: Callable[[table_name], pandas.DataFrame(columns=[...])]

    RDBMS mode: introspects Postgres information_schema.
    PARQUET mode: treats PARQUET_PATH as a *Delta Lake root* (e.g. /lake) and discovers
                  per-table directories containing _delta_log/.
    """
    import os
    import json
    import pyarrow.parquet as pq  # kept to preserve import side-effects (compat)

    if LAKE_TYPE == "rdbms":
        from psycopg2 import sql

        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (RDBMS_SCHEMA,),
        )
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
        release_postgres_connection(conn)

        def load_table(t: str) -> pd.DataFrame:
            conn2 = connect_postgres()
            cur2 = conn2.cursor()

            q = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
                sql.Identifier(RDBMS_SCHEMA), sql.Identifier(t)
            )
            cur2.execute(q)
            cols = [desc[0] for desc in cur2.description]
            cur2.close()
            conn2.close()
            return pd.DataFrame(columns=cols)

        return tables, load_table

    # ------------------------------
    # PARQUET MODE (Delta root)
    # ------------------------------
    lake_root = Path(PARQUET_PATH)

    if not lake_root.exists():
        log.warning(f"[DVH_CORE][discover_lake] PARQUET_PATH does not exist: {lake_root}")
        return [], (lambda _: pd.DataFrame())

    # Discover delta tables in root (case-insensitive keys).
    table_dirs = [p for p in lake_root.iterdir() if is_delta_table_dir(p)]
    dir_by_key = {p.name.lower(): p for p in table_dirs}
    tables = sorted(dir_by_key.keys())

    def _normalize_table_key(name: str) -> str:
        name = (name or "").strip()
        if "." in name:
            name = name.split(".", 1)[-1]
        return name.lower()

    def load_table(table_name: str) -> pd.DataFrame:
        key = _normalize_table_key(table_name)
        table_dir = dir_by_key.get(key)

        if table_dir is None:
            # last resort: scan (covers unexpected casing / odd characters)
            for p in table_dirs:
                if p.name.lower() == key:
                    table_dir = p
                    break

        if table_dir is None or not is_delta_table_dir(table_dir):
            log.warning(
                f"[DVH_CORE][discover_lake] Table not found or not a delta table: requested={table_name!r} "
                f"(key={key!r}) under root={lake_root}"
            )
            return pd.DataFrame()

        cols = schema_from_delta_log(table_dir)
        if cols is None:
            cols = fallback_schema_from_parquet(table_dir)

        if not cols:
            return pd.DataFrame()

        return pd.DataFrame(columns=cols)

    return tables, load_table
