"""Hive schema helper.

Extracted from the original main.py without behavioral changes.
"""

import os
from pathlib import Path

import yaml
from jinja2 import Template
from logger import log
from pyhive import hive

from config import (
    DBT_PROFILES_DIR,
    STAGING_SCHEMA,
    RAW_VAULT_SCHEMA,
    STAGING_BASE_PATH,
    RAW_VAULT_BASE_PATH,
)
from helper.hive_helper import resolve_thrift, render_profile_value


def ensure_database_schema():
    """Create target Spark database/schema with a LOCATION if configured."""
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        return

    with open(profiles_yml_path, "r", encoding="utf-8") as f:
        profiles = yaml.safe_load(f) or {}

    default_profile = profiles.get("default", {})
    target = default_profile.get("target")
    outputs = default_profile.get("outputs", {})
    target_cfg = outputs.get(target, {})

    schema = render_profile_value(target_cfg.get("schema") or target_cfg.get("database"))
    if not schema:
        return
    schema = str(schema).strip()
    if not schema or "{{" in schema or "}}" in schema:
        schema = RAW_VAULT_SCHEMA

    # Map known schemas to their base paths
    schema_locations = {
        STAGING_SCHEMA: STAGING_BASE_PATH,
        RAW_VAULT_SCHEMA: RAW_VAULT_BASE_PATH,
    }
    desired_loc = schema_locations.get(schema)

    host, port, user = resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user)
    cursor = conn.cursor()

    if desired_loc:
        Path(desired_loc).mkdir(parents=True, exist_ok=True)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{desired_loc}'")
        log.info(f"[DVH_HELPER][DB] Ensured database/schema '{schema}' exists at {desired_loc}")
    else:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
        log.info(f"[DVH_HELPER][DB] Ensured database/schema '{schema}' exists")

    if desired_loc:
        cursor.execute(f"DESCRIBE DATABASE EXTENDED {schema}")
        rows = cursor.fetchall()
        current_loc = next((r[1] for r in rows if str(r[0]).lower() == "location"), None)
        if current_loc and current_loc.rstrip("/") != desired_loc.rstrip("/"):
            cursor.execute(f"ALTER DATABASE {schema} SET LOCATION '{desired_loc}'")
            log.info(f"[DVH_HELPER][DB] Moved default LOCATION of {schema} to {desired_loc}")

    cursor.close()
    conn.close()
