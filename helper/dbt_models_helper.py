"""dbt model file helpers.

Extracted from the original main.py without behavioral changes.
"""

import json
import os
from pathlib import Path

import pandas as pd
import yaml
from jinja2 import Template
from logger import log

from config import (
    LAKE_TYPE,
    DBT_MODELS_SQL_DIR,
    DBT_MODELS_JSON_DIR,
    STAGING_SCHEMA,
    RAW_VAULT_SCHEMA,
)
from helper.filesystem_helper import write_text_if_changed
from meta_store import write_metadata, write_lineage
from dv_modeller import extract_metadata, split_datavault

def write_sql_model_file(model_name, table_name, model_type, meta):
    """Create/update a dbt SQL model file based on JSON metadata (idempotent)."""
    os.makedirs(DBT_MODELS_SQL_DIR, exist_ok=True)
    file_path = os.path.join(DBT_MODELS_SQL_DIR, f"{model_name}.sql")

    # --- normalize inputs ---------------------------------------------------
    mtype = (model_type or "").lower()
    if mtype in {"satellite", "sat"}:
        mtype = "sat"
    elif mtype not in {"hub", "link"}:
        raise ValueError(f"Unsupported model_type: {model_type!r}")

    business_keys = list(meta.get("business_keys", []))
    attributes = list(meta.get("attributes", []))
    attrs = [a for a in attributes if a not in business_keys]
    src_name = meta.get("source_name") or "staging"

    # --- config block: literal unique_key + merge ---------------------------
    # NOTE: For satellites we merge on (business_keys + hashdiff). Delta MERGE fails if
    # the source contains duplicate rows for the same unique_key. We therefore deduplicate
    # the source (SELECT DISTINCT) in the SAT query below.
    unique_key = (business_keys + ["hashdiff"]) if mtype == "sat" else business_keys
    if (mtype in {"hub", "link"}) and not business_keys:
        raise ValueError(f"{mtype} model requires business_keys")

    config_lines = [
        "materialized='incremental'",
        "file_format='delta'",
        "on_schema_change='append_new_columns'",
        "incremental_strategy='merge'",
        f"unique_key={unique_key!r}",
    ]
    incremental_conf = "{{ config(\n  " + ",\n  ".join(config_lines) + "\n) }}\n"

    # helper to keep jinja braces intact
    def jinja_source(src, tbl):
        return "{{ source('" + src + "', '" + tbl + "') }}"

    src_tbl = str(table_name).lower()

    # --- HUB / LINK ---------------------------------------------------------
    if mtype in {"hub", "link"}:
        lines = [incremental_conf, "select distinct"]

        # keys + audit
        for k in business_keys:
            lines.append("    " + k + ",")
        lines.append("    current_timestamp() as load_datetime,")
        lines.append("    '" + table_name + "' as record_source")
        lines.append("from " + jinja_source(src_name, src_tbl))
        lines.append("group by " + ", ".join(business_keys))

        content = "\n".join(lines) + "\n"
        wrote = write_text_if_changed(file_path, content)
        if wrote:
            log.debug(f'[DVH_HELPER][DBT] Wrote SQL model {model_name}.sql')
        return wrote

    # --- SATELLITE ----------------------------------------------------------
    # SAT models need deduplication to keep Delta MERGE happy:
    #   - Delta MERGE errors if multiple source rows match the same target unique key
    #   - unique_key is (business_keys + hashdiff)
    # We dedupe by selecting DISTINCT on (business_keys + attrs + hashdiff) in a CTE,
    # then add audit columns outside the DISTINCT.
    select_cols = []
    for c in business_keys + attrs:
        if c not in select_cols:
            select_cols.append(c)

    if attrs:
        attrs_expr = ", ".join("coalesce(cast(" + c + " as string), '')" for c in attrs)
        hashdiff_expr = "sha2(concat_ws('||', " + attrs_expr + "), 256)"
    else:
        hashdiff_expr = "sha2('', 256)"

    cte_select_parts = select_cols + [f"{hashdiff_expr} as hashdiff"]
    cte_select_sql = ",\n    ".join(cte_select_parts)

    final_select_parts = select_cols + [
        "hashdiff",
        "current_timestamp() as load_datetime",
        "'" + table_name + "' as record_source",
    ]
    final_select_sql = ",\n    ".join(final_select_parts)

    lines = [
        incremental_conf,
        "with src as (",
        "  select distinct",
        "    " + cte_select_sql,
        "  from " + jinja_source(src_name, src_tbl),
        ")",
        "select",
        "    " + final_select_sql,
        "from src",
    ]

    content = "\n".join(lines) + "\n"
    wrote = write_text_if_changed(file_path, content)
    if wrote:
        log.debug(f'[DVH_HELPER][DBT] Wrote SQL model {model_name}.sql')
    return wrote

def write_json_model_file(model_name, table_name, model_type, meta):
    """Persist model metadata as JSON for dbt-spark (idempotent) and sync SQL."""
    os.makedirs(DBT_MODELS_JSON_DIR, exist_ok=True)
    file_path = os.path.join(DBT_MODELS_JSON_DIR, f"{model_name}.json")

    # Normalize type once so JSON + SQL stay consistent
    mtype = (model_type or "").lower()
    if mtype in {"satellite", "sat"}:
        mtype = "sat"
    elif mtype not in {"hub", "link"}:
        raise ValueError(f"Unsupported model_type: {model_type!r}")

    bks = list(meta.get("business_keys", []))

    attrs_raw = list(meta.get("attributes", []))
    attrs = []
    for a in attrs_raw:
        if a not in bks and a not in attrs:
            attrs.append(a)

    cols_raw = list(meta.get("columns", []))
    cols = []
    for c in cols_raw:
        if c not in cols:
            cols.append(c)


    model_def = {
        "model_name": model_name,
        "table_name": table_name,
        "model_type": mtype,
        "business_keys": bks,
        "attributes": attrs,
        "columns": cols,
    }

    json_txt = json.dumps(model_def, indent=2) + "\n"
    wrote_json = write_text_if_changed(file_path, json_txt)
    wrote_sql = write_sql_model_file(model_name, table_name, mtype, model_def)
    if wrote_json or wrote_sql:
        write_metadata(model_def)
        write_lineage(
            {
                "source_table": table_name,
                "target_model": model_name,
                "model_type": mtype,
                "business_keys": model_def["business_keys"],
                "attributes": model_def["attributes"],
                "columns": model_def["columns"],
            }
        )
        log.info(f'[DVH_HELPER][GEN] Generated/updated DBT JSON model for {model_name} (from lake table {table_name})')

def generate_schema_yml(table_names, output_path=None):
    table_names = list(table_names or [])

    if output_path is None:
        output_path = str(Path(DBT_MODELS_SQL_DIR).resolve().parent / "schema.yml")

    lines = []
    lines.append("version: 2")
    lines.append("")
    lines.append("sources:")
    lines.append("  - name: staging")
    lines.append('    schema: "{{ env_var(\'STAGING_SCHEMA\', \'bronze\') }}"')

    if table_names:
        lines.append("    tables:")
        for t in table_names:
            lines.append(f"      - name: {str(t).lower()}")
    else:
        lines.append("    tables: []")

    write_text_if_changed(output_path, "\n".join(lines) + "\n")

def ensure_dbt_models_for_lake(tables, load_table):
    """Generate JSON/SQL model metadata for each lake table (idempotent)."""
    new_models = []
    for table in tables:
        df_schema = load_table(table)
        meta = extract_metadata(table, df_schema)
        hubs, links, sats = split_datavault(table, meta)

        # Hubs
        for hub in hubs:
            model_name = hub["name"]
            bk = hub["key"]
            write_json_model_file(
                model_name, table, "hub", {"business_keys": bk, "attributes": [], "columns": bk}
            )
            new_models.append(model_name)

        # Links
        for link in links:
            model_name = link["name"]
            keys = link["keys"]
            write_json_model_file(
                model_name, table, "link", {"business_keys": keys, "attributes": [], "columns": keys}
            )
            new_models.append(model_name)

        # Satellites
        for sat in sats:
            model_name = sat["name"]
            keys = sat["key"]
            atts = sat["attributes"]
            write_json_model_file(
                model_name, table, "sat", {"business_keys": keys, "attributes": atts, "columns": keys + atts}
            )
            new_models.append(model_name)
    return new_models

def get_existing_model_tables():
    """Return mapping of lake tables to their generated model names (no rewrites)."""
    table_models = {}
    if not os.path.exists(DBT_MODELS_JSON_DIR):
        return table_models
    for fname in os.listdir(DBT_MODELS_JSON_DIR):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(DBT_MODELS_JSON_DIR, fname), "r") as f:
            data = json.load(f)
        table_name = data.get("table_name")
        model_name = data.get("model_name")
        table_models.setdefault(table_name, []).append(model_name)
    return table_models


