"""Parquet/Delta Lake helper functions.

This isolates lake schema discovery helpers used by core.lake_discovery.
Functionality is unchanged; implementations were previously nested inside discover_lake().
"""

import json
from pathlib import Path
from typing import Optional, List

import pyarrow.parquet as pq
from logger import log


def is_delta_table_dir(p: Path) -> bool:
    return p.is_dir() and (p / "_delta_log").is_dir()


def latest_delta_log_json(delta_log_dir: Path) -> Optional[Path]:
    """Pick the highest-numbered Delta commit JSON file, if any."""
    json_files = sorted(delta_log_dir.glob("*.json"))
    return json_files[-1] if json_files else None


def schema_from_delta_log(table_dir: Path) -> Optional[List[str]]:
    """Parse schemaString from the delta commit JSON."""
    delta_log_dir = table_dir / "_delta_log"
    commit = latest_delta_log_json(delta_log_dir)
    if not commit:
        return None

    try:
        with commit.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                md = obj.get("metaData")
                if md and "schemaString" in md:
                    schema_str = md["schemaString"]
                    schema_obj = json.loads(schema_str)
                    fields = schema_obj.get("fields", [])
                    return [fld.get("name") for fld in fields if fld.get("name")]
    except Exception as exc:
        log.warning(f"[DVH_HELPER][discover_lake] Failed to parse delta log schema for {table_dir}: {exc}")

    return None


def fallback_schema_from_parquet(table_dir: Path) -> Optional[List[str]]:
    """Fallback: find any parquet file under the table directory and read metadata schema."""
    try:
        for pf in table_dir.rglob("*.parquet"):
            pf = pf.resolve()
            meta = pq.read_metadata(str(pf))
            return meta.schema.names
    except Exception as exc:
        log.warning(f"[DVH_HELPER][discover_lake] Fallback parquet schema failed for {table_dir}: {exc}")
    return None
