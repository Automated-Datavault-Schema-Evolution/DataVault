"""dbt runner helpers.

Extracted from the original main.py without behavioral changes.
"""

import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Iterable, Set, Optional

import pandas as pd
import yaml
from logger import log

from config import (
    DBT_MODELS_SQL_DIR,
    DBT_PROFILES_DIR,
    DBT_MODELS_JSON_DIR,
    LAKE_TYPE,
    STAGING_SCHEMA,
    RAW_VAULT_SCHEMA,
)

from helper.spark_helper import get_spark_session
from helper.postgres_helper import connect_postgres, release_postgres_connection
from helper.dbt_profiles_helper import ensure_profiles_dir
from core.bronze_preflight import _preflight_bronze_for_tables
from helper.dbt_models_helper import generate_schema_yml

_DBT_LOCK = threading.Lock()  # serializes actual dbt subprocess runs
_DBT_PREFLIGHT_SPARK = None
_DBT_PREFLIGHT_LOCK = threading.Lock()

def _get_dbt_preflight_spark():
    global _DBT_PREFLIGHT_SPARK
    if _DBT_PREFLIGHT_SPARK is None:
        with _DBT_PREFLIGHT_LOCK:
            if _DBT_PREFLIGHT_SPARK is None:
                _DBT_PREFLIGHT_SPARK = get_spark_session("DataVault_DBT_Preflight")
    return _DBT_PREFLIGHT_SPARK


def run_dbt_models(models):
    """Run dbt for the specified models."""
    if not models:
        return

    ensure_profiles_dir()

    repo_root = Path(__file__).resolve().parent.parent
    schema_path = str(repo_root / "models" / "schema.yml")

    referenced_tables = set()
    for m in models:
        meta_path = os.path.join(DBT_MODELS_JSON_DIR, f"{m}.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                j = json.load(f)
            t = (j.get("table_name") or "").strip()
            if t:
                referenced_tables.add(t)
        except FileNotFoundError:
            continue
        except Exception as e:
            log.warning(f'[DVH_HELPER][DBT] Could not read model metadata for {m}: {e}')

    # Normalize table ids to match how bronze is actually named/registered
    referenced_tables = {str(t).strip().lower() for t in referenced_tables if str(t).strip()}

    # NEW: Ensure bronze sources exist + have the expected columns before dbt reads them
    _preflight_bronze_for_tables(sorted(referenced_tables))

    # Merge with existing schema.yml tables (avoid thrash)
    existing_tables = set()
    try:
        if os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as f:
                doc = yaml.safe_load(f) or {}
            for src in (doc.get("sources") or []):
                if (src.get("name") == "staging") and isinstance(src.get("tables"), list):
                    for t in src["tables"]:
                        if isinstance(t, dict) and t.get("name"):
                            existing_tables.add(str(t["name"]).strip().lower())
    except Exception as e:
        log.warning(f'[DVH_HELPER][DBT] Could not parse existing schema.yml (will regenerate): {e}')

    all_model_tables = set()
    try:
        if os.path.exists(DBT_MODELS_JSON_DIR):
            for fname in os.listdir(DBT_MODELS_JSON_DIR):
                if not fname.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(DBT_MODELS_JSON_DIR, fname), "r", encoding="utf-8") as f:
                        j = json.load(f)
                    t = (j.get("table_name") or "").strip()
                    if t:
                        all_model_tables.add(str(t).strip().lower())
                except Exception:
                    # Best-effort: ignore malformed/partial files
                    continue
    except Exception as e:
        log.warning(f'[DVH_HELPER][DBT] Could not scan model metadata directory for schema sources: {e}')

    merged = sorted(existing_tables.union(referenced_tables).union(all_model_tables))
    generate_schema_yml(merged, output_path=schema_path)

    # Allow benchmarks to control dbt parallelism (important for Delta/Parquet stability).
    # Priority: E2E_DBT_THREADS -> DBT_THREADS -> default 4
    threads_raw = (os.environ.get("E2E_DBT_THREADS") or os.environ.get("DBT_THREADS") or "").strip()
    try:
        threads = int(threads_raw) if threads_raw else 4
    except Exception:
        threads = 4

    cmd = [
        "dbt",
        "run",
        "--profiles-dir",
        DBT_PROFILES_DIR,
        "--threads",
        str(threads),
        "--select",
    ] + sorted(models)
    log.info(f'[DVH_HELPER][DBT] Running: {cmd}')

    with _DBT_LOCK:
        proc = subprocess.run(cmd, capture_output=True, text=True, env={**os.environ, "DBT_THREADS": str(threads)})
        if proc.stdout:
            log.info(proc.stdout)
        if proc.stderr:
            log.error(proc.stderr)

        if proc.returncode != 0:
            raise RuntimeError(f"dbt failed (rc={proc.returncode})")

