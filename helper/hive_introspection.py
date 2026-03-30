"""Hive table introspection helpers.

Extracted from the original main.py without behavioral changes.
"""

import os

import yaml
from logger import log
from pyhive import hive

from config import DBT_PROFILES_DIR, RAW_VAULT_SCHEMA
from helper.hive_helper import resolve_thrift, render_profile_value


def get_raw_vault_tables():
    """List existing tables in the raw vault."""
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        return set()
    with open(profiles_yml_path, "r", encoding="utf-8") as f:
        profiles = yaml.safe_load(f) or {}
    default_profile = profiles.get("default", {})
    target = default_profile.get("target")
    outputs = default_profile.get("outputs", {})
    target_cfg = outputs.get(target, {})
    schema = render_profile_value(target_cfg.get("schema") or target_cfg.get("database") or RAW_VAULT_SCHEMA)
    if not schema:
        return set()
    schema = str(schema).strip()
    if not schema or "{{" in schema or "}}" in schema:
        schema = RAW_VAULT_SCHEMA
    host, port, user = resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SHOW TABLES IN {schema}")
        tables = [row[0] for row in cursor.fetchall()]
        return set(tables)
    except Exception as e:
        msg = str(e).lower()
        if any(x in msg for x in ("parse_syntax_error", "database not found", "schema not found", "no such database", "unknown database")):
            log.warning(f"[DVH_HELPER][HIVE] Raw vault schema '{schema}' is not queryable yet: {e}")
            return set()
        raise
    finally:
        cursor.close()
        conn.close()
