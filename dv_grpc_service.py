# vault_grpc_service.py

from __future__ import annotations

import json
import os
import re
from concurrent import futures
from typing import Dict, Any, List, Tuple, Optional

import grpc
import threading
import time
from logger import log
from config import DBT_MODELS_JSON_DIR
from meta_store import write_metadata
from proto import sef_handlers_pb2 as pb
from proto import sef_handlers_pb2_grpc as pb_grpc
from dv_modeller import extract_metadata, split_datavault
from main import (
    discover_lake,
    write_json_model_file,
    run_dbt_models,
)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
def _is_transient_dbt_failure(msg: str) -> bool:
    m = (msg or "").lower()

    # Typical "eventual consistency" errors: bronze table not created yet, schema not applied yet,
    # Spark metastore not updated yet, temporary connectivity issues.
    transient_markers = [
        "[dbt]",
        "dbt run failed",
        "table or view not found",
        "table_or_view_not_found",
        "unresolved_relation",
        "unresolved_column",
        "analysisexception",
        "no such table",
        "does not exist",
        "not found",
        "path does not exist",
        "metadatachangedexception",
        "concurrentmodificationexception",
        "timeout",
        "timed out",
        "connection refused",
        "temporarily unavailable",
        "dbt failed"
    ]
    return any(x in m for x in transient_markers)

def _make_evidence_id(operation: pb.Operation) -> str:
    target = operation.target or "unknown"
    plan_id = operation.plan_id or "no_plan"
    idem = operation.idempotency_key or "no_idem"
    return f"vault:{target}:{plan_id}:{idem}"


def _load_table_schema(table_name: str):
    """
    Use the existing discover_lake() to get a schema DataFrame for a table.
    """
    tables, load_table = discover_lake()
    if table_name not in tables:
        raise ValueError(f"Table {table_name!r} not found in lake tables {tables}")
    return load_table(table_name)


def _load_models_for_table(table_name: str) -> List[Dict[str, Any]]:
    """
    Load all dbt JSON model definitions for a given lake table.
    """
    models: List[Dict[str, Any]] = []
    if not os.path.isdir(DBT_MODELS_JSON_DIR):
        return models

    for fname in os.listdir(DBT_MODELS_JSON_DIR):
        if not fname.endswith(".json"):
            continue
        path = os.path.join(DBT_MODELS_JSON_DIR, fname)
        try:
            with open(path, "r") as f:
                model = json.load(f)
        except Exception:
            continue
        if model.get("table_name") != table_name:
            continue
        models.append(model)
    return models


def _models_by_type(models: List[Dict[str, Any]], mtype: str) -> List[Dict[str, Any]]:
    mt = mtype.lower()
    return [m for m in models if (m.get("model_type") or "").lower() == mt]


def _next_versioned_name(base_name: str, existing_names: List[str]) -> str:
    """
    Return a non-conflicting model name.

    - If base_name not in existing_names -> base_name.
    - If base_name already exists -> base_name_v2, base_name_v3, ...
    """
    if base_name not in existing_names:
        return base_name

    pattern = re.compile(re.escape(base_name) + r"_v(\d+)$")
    max_v = 1
    for name in existing_names:
        m = pattern.fullmatch(name)
        if m:
            try:
                v = int(m.group(1))
                max_v = max(max_v, v)
            except ValueError:
                continue
    return f"{base_name}_v{max_v + 1}"


def _discover_and_split(table_name: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Helper that:
      - loads the table schema,
      - extracts DV metadata,
      - splits into hubs, links, satellites.
    """
    df_schema = _load_table_schema(table_name)
    meta = extract_metadata(table_name, df_schema)
    hubs, links, sats = split_datavault(table_name, meta)
    return meta, hubs, links, sats

def _is_transient_discovery_error(exc: Exception) -> bool:
    s = str(exc).lower()
    # “lake table not present yet” / “introspection raced ingestion”
    if "not found in lake tables" in s:
        return True
    # RDBMS introspection races can also surface like this in some paths
    if "relation" in s and "does not exist" in s:
        return True
    return False

# ---------------------------------------------------------------------------
# Satellite evolution helpers
# ---------------------------------------------------------------------------


def _ensure_satellite_for_table(
    table_name: str,
    sat_template: Dict[str, Any],
    all_models_for_table: List[Dict[str, Any]],
) -> Tuple[List[str], bool]:
    """
    Ensure there is at least one satellite model that matches the sat_template
    (key + attributes) for this table.

    Returns:
        (created_models, already_present_flag)
    """
    existing_sats = _models_by_type(all_models_for_table, "sat")
    target_keys = list(sat_template.get("key") or [])
    target_attrs = list(sat_template.get("attributes") or [])

    # Check if a satellite with the same shape already exists
    for s in existing_sats:
        bk = list(s.get("business_keys") or [])
        atts = list(s.get("attributes") or [])
        if sorted(bk) == sorted(target_keys) and sorted(atts) == sorted(target_attrs):
            # Already have a compatible satellite
            return [], True

    # Need a new satellite
    base_name = sat_template.get("name") or f"sat_{table_name}"
    existing_names = [s["model_name"] for s in existing_sats if "model_name" in s]
    new_name = _next_versioned_name(base_name, existing_names)

    meta_sat = {
        "business_keys": target_keys,
        "attributes": target_attrs,
        "columns": target_keys + target_attrs,
    }

    write_json_model_file(new_name, table_name, "sat", meta_sat)
    return [new_name], False


# ---------------------------------------------------------------------------
# Operation handlers
# ---------------------------------------------------------------------------


def _handle_add_column_for_vault(operation: pb.Operation) -> pb.OperationResult:
    """
    Extend an existing satellite for a table by adding a new attribute column.

    This is the "regular" evolution path for new descriptive columns.
    """
    params = dict(operation.params)
    column_name = params.get("column_name")
    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_ADD_COLUMN",
        )

    table_name = operation.target
    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_ADD_COLUMN",
        )

    # The "default" satellite for this table is sat_<table_base>;
    # we evolve that by appending the new column as an attribute.
    meta, hubs, links, sats = _discover_and_split(table_name)

    if not sats:
        msg = f"[GRPC_SERVICE] No satellite template available for table {table_name!r}; cannot apply ADD_COLUMN in vault"
        log.error(msg)
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="NO_SAT_TEMPLATE",
            error_message=msg,
        )

    sat_template = sats[0]
    attributes = list(sat_template.get("attributes") or [])
    if column_name in attributes:
        # Already present -> idempotent no-op
        evidence_id = _make_evidence_id(operation)
        try:
            write_metadata(
                {
                    "layer": "vault",
                    "target": sat_template["name"],
                    "plan_id": operation.plan_id,
                    "correlation_id": operation.correlation_id,
                    "idempotency_key": operation.idempotency_key,
                    "operation_kind": "ADD_COLUMN",
                    "params": params,
                    "evidence_id": evidence_id,
                    "source": "vault_handler_grpc",
                    "note": "no-op; column already present",
                }
            )
        except Exception as exc:
            log.critical(f"[GRPC_SERVICE] Failed to write metadata for ALREADY_APPLIED op: {exc}")

        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_ALREADY_APPLIED,
            error_code="",
            error_message="",
            evidence_snapshot_id=evidence_id,
        )

    # Add to attributes and regenerate model
    attributes.append(column_name)
    sat_template["attributes"] = attributes

    models_for_table = _load_models_for_table(table_name)
    created_models, _ = _ensure_satellite_for_table(table_name, sat_template, models_for_table)

    evidence_id = _make_evidence_id(operation)

    try:
        if created_models:
            run_dbt_models(created_models)

        write_metadata(
            {
                "layer": "vault",
                "target": sat_template["name"],
                "plan_id": operation.plan_id,
                "correlation_id": operation.correlation_id,
                "idempotency_key": operation.idempotency_key,
                "operation_kind": "ADD_COLUMN",
                "params": params,
                "evidence_id": evidence_id,
                "source": "vault_handler_grpc",
            }
        )
        status = pb.OPERATION_STATUS_OK
        error_code = ""
        error_message = ""
    except Exception as exc:
        msg = str(exc)
        log.critical(f"[GRPC_SERVICE] Error applying ADD_COLUMN in vault for table {table_name}: {msg}")

        if _is_transient_dbt_failure(msg):
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
            error_code = "VAULT_ADD_COLUMN_TRANSIENT"
        else:
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "VAULT_ADD_COLUMN_FAILED"

        error_message = msg

    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=evidence_id,
    )


def _handle_new_hub_for_vault(operation: pb.Operation) -> pb.OperationResult:
    """
    Create a new hub for a table, *without* altering existing hubs.

    Scenarios:
      - If no hub exists yet for the table -> create the base hub (hub_<base>).
      - If a hub exists but the inferred business key set would change ->
        create a *new* hub variant (hub_<base>_v2, v3, ...) instead of
        changing the existing one. Then ensure a compatible satellite exists.
    """
    table_name = operation.target or dict(operation.params).get("table_name")
    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_NEW_HUB",
        )

    try:
        meta, hubs, links, sats = _discover_and_split(table_name)
    except Exception as exc:
        msg = f"[GRPC_SERVICE] Failed to discover DV metadata for table {table_name!r}: {exc}"
        transient = _is_transient_discovery_error(exc)

        if transient:
            log.warning(msg)
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
        else:
            log.critical(msg)
            status = pb.OPERATION_STATUS_PERMANENT_ERROR

        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=status,
            error_code="DV_DISCOVERY_FAILED",
            error_message=msg,
        )

    if not hubs:
        msg = f"[GRPC_SERVICE] No hub candidate detected for table {table_name!r}; cannot create hub"
        log.error(msg)
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="NO_HUB_CANDIDATE",
            error_message=msg,
        )

    hub_candidate = hubs[0]
    hub_keys = list(hub_candidate.get("key") or [])
    base_hub_name = hub_candidate.get("name") or f"hub_{table_name}"

    models = _load_models_for_table(table_name)
    existing_hubs = _models_by_type(models, "hub")

    existing_names = [m["model_name"] for m in existing_hubs if "model_name" in m]

    # Check if a hub with the same key set already exists
    for h in existing_hubs:
        bk = list(h.get("business_keys") or [])
        if sorted(bk) == sorted(hub_keys):
            # Already have a hub with this shape. We still want satellites to fit.
            log.info(
                f"[GRPC_SERVICE] Hub with same keyset already exists for table {table_name} (model=%s); no new hub created",
                h.get("model_name"),
            )
            hub_model_name = h.get("model_name") or base_hub_name
            break
    else:
        # Need a new hub: either first hub, or a side-by-side variant
        hub_model_name = _next_versioned_name(base_hub_name, existing_names)
        meta_hub = {
            "business_keys": hub_keys,
            "attributes": [],
            "columns": hub_keys,
        }
        write_json_model_file(hub_model_name, table_name, "hub", meta_hub)
        log.info(
            f"[GRPC_SERVICE] Created new hub model {hub_model_name} for table {table_name} with business_keys={hub_keys}")

    models = _load_models_for_table(table_name)  # refresh for satellites

    # Ensure there is a compatible satellite that "accepts" this hub
    if sats:
        sat_template = sats[0]
        created_models, already_present = _ensure_satellite_for_table(table_name, sat_template, models)
    else:
        created_models, already_present = [], True

    evidence_id = _make_evidence_id(operation)

    try:
        if created_models:
            run_dbt_models([hub_model_name] + created_models)
        else:
            # Only hub might be new
            if hub_model_name not in [m.get("model_name") for m in existing_hubs]:
                run_dbt_models([hub_model_name])

        write_metadata(
            {
                "layer": "vault",
                "target": hub_model_name,
                "plan_id": operation.plan_id,
                "correlation_id": operation.correlation_id,
                "idempotency_key": operation.idempotency_key,
                "operation_kind": "NEW_HUB",
                "params": dict(operation.params),
                "evidence_id": evidence_id,
                "source": "vault_handler_grpc",
            }
        )

        status = (
            pb.OPERATION_STATUS_ALREADY_APPLIED
            if (hub_model_name in existing_names and already_present)
            else pb.OPERATION_STATUS_OK
        )
        error_code = ""
        error_message = ""
    except Exception as exc:
        msg = str(exc)
        log.critical(f"[GRPC_SERVICE] Error applying NEW_HUB in vault for table {table_name}: {exc}")

        if _is_transient_dbt_failure(msg):
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
            error_code = "VAULT_NEW_HUB_FAILED_TRANSIENT"
        else:
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "VAULT_NEW_HUB_FAILED_PERMANENT"

        error_message = msg

    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=evidence_id,
    )


def _handle_new_link_for_vault(operation: pb.Operation) -> pb.OperationResult:
    """
    Create new link(s) for a table, without altering existing links.

    Scenarios:
      - If no link exists yet for the inferred foreign key(s) -> create base link(s).
      - If an inferred link's keyset would differ from existing link(s) -> create a
        side-by-side variant (link_<base>_<fk_base>_v2, v3, ...).
      - Satellites are left as-is unless the changed modelling also yields a new
        satellite template, in which case we ensure a compatible satellite exists.
    """
    params = dict(operation.params)
    table_name = operation.target or params.get("table_name")
    fk_filter = params.get("fk_column")  # optional: limit to a single FK

    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_NEW_LINK",
        )

    try:
        meta, hubs, links, sats = _discover_and_split(table_name)
    except Exception as exc:
        msg = f"[GRPC_SERVICE] Failed to discover DV metadata for table {table_name!r}: {exc}"
        transient = _is_transient_discovery_error(exc)

        if transient:
            log.warning(msg)
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
        else:
            log.critical(msg)
            status = pb.OPERATION_STATUS_PERMANENT_ERROR

        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=status,
            error_code="DV_DISCOVERY_FAILED",
            error_message=msg,
        )

    candidates: List[Dict[str, Any]] = []
    for l in links:
        keys = list(l.get("keys") or [])
        if fk_filter and (len(keys) < 2 or keys[1] != fk_filter):
            continue
        candidates.append(l)

    if not candidates:
        msg = f"[GRPC_SERVICE] No link candidates found for table {table_name!r} (fk_filter={fk_filter!r})"
        log.warnin(msg)
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_ALREADY_APPLIED,
            error_code="NO_LINK_CANDIDATE",
            error_message="",
            evidence_snapshot_id=_make_evidence_id(operation),
        )

    models = _load_models_for_table(table_name)
    existing_links = _models_by_type(models, "link")

    created_models: List[str] = []
    existing_names = [m["model_name"] for m in existing_links if "model_name" in m]

    for cand in candidates:
        keys = list(cand.get("keys") or [])
        base_link_name = cand.get("name") or f"link_{table_name}"

        # Does a compatible link exist?
        found_same = False
        for l in existing_links:
            bk = list(l.get("business_keys") or [])
            if sorted(bk) == sorted(keys):
                found_same = True
                break

        if found_same:
            log.info(
                f"[GRPC_SERVICE] Link with same keyset already exists for table {table_name} (model=%s); "
                f"no new link created for candidate {base_link_name}",
                l.get("model_name"),
            )
            continue

        # Create a side-by-side link (either first or a versioned variant)
        link_model_name = _next_versioned_name(base_link_name, existing_names)
        meta_link = {
            "business_keys": keys,
            "attributes": [],
            "columns": keys,
        }
        write_json_model_file(link_model_name, table_name, "link", meta_link)
        created_models.append(link_model_name)
        existing_names.append(link_model_name)
        log.info(
            f"[GRPC_SERVICE] Created new link model {link_model_name} for table {table_name} with business_keys={keys}")

    evidence_id = _make_evidence_id(operation)

    try:
        if created_models:
            run_dbt_models(created_models)

        write_metadata(
            {
                "layer": "vault",
                "target": table_name,
                "plan_id": operation.plan_id,
                "correlation_id": operation.correlation_id,
                "idempotency_key": operation.idempotency_key,
                "operation_kind": "NEW_LINK",
                "params": params,
                "evidence_id": evidence_id,
                "source": "vault_handler_grpc",
            }
        )

        status = pb.OPERATION_STATUS_OK if created_models else pb.OPERATION_STATUS_ALREADY_APPLIED
        error_code = ""
        error_message = ""
    except Exception as exc:
        msg = str(exc)
        log.critical(f"[GRPC_SERVICE] Error applying NEW_LINK in vault for table {table_name}: {exc}")

        if _is_transient_dbt_failure(msg):
            status = pb.OPERATION_STATUS_TRANSIENT_ERROR
            error_code = "VAULT_NEW_LINK_TRANSIENT"
        else:
            status = pb.OPERATION_STATUS_PERMANENT_ERROR
            error_code = "VAULT_NEW_LINK_FAILED_PERMANENT"
    return pb.OperationResult(
        correlation_id=operation.correlation_id,
        plan_id=operation.plan_id,
        idempotency_key=operation.idempotency_key,
        status=status,
        error_code=error_code,
        error_message=error_message,
        evidence_snapshot_id=evidence_id,
    )

def _handle_change_type_for_vault(operation: pb.Operation) -> pb.OperationResult:
    """
    Vault is non-destructive. For CHANGE_TYPE we:
      - record evidence
      - rerun dbt models for the table (safe, idempotent)
    """
    params = dict(operation.params)
    column_name = params.get("column_name")
    table_name = operation.target

    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_CHANGE_TYPE",
        )

    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_CHANGE_TYPE",
        )

    evidence_id = _make_evidence_id(operation)

    try:
        models = _load_models_for_table(table_name)
        model_names = [m.get("model_name") for m in models if m.get("model_name")]
        if model_names:
            run_dbt_models(model_names)

        write_metadata(
            {
                "layer": "vault",
                "target": table_name,
                "plan_id": operation.plan_id,
                "correlation_id": operation.correlation_id,
                "idempotency_key": operation.idempotency_key,
                "operation_kind": "CHANGE_TYPE",
                "params": params,
                "evidence_id": evidence_id,
                "models_rerun": model_names,
                "note": "non-destructive; reran models to align with lake typing",
            }
        )

        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_OK,
            error_code="",
            error_message="",
            evidence_snapshot_id=evidence_id,
        )
    except Exception as exc:
        msg = f"Vault CHANGE_TYPE failed for {table_name}.{column_name}: {exc}"
        log.exception("[GRPC_SERVICE] %s", msg)
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_TRANSIENT_ERROR,
            error_code="DV_CHANGE_TYPE_FAILED",
            error_message=msg,
            evidence_snapshot_id=evidence_id,
        )


def _handle_drop_column_for_vault(operation: pb.Operation) -> pb.OperationResult:
    """
    Non-destructive handling of DROP_COLUMN in the vault:
      - Re-discover DV templates from the current lake schema.
      - Ensure side-by-side variants exist where shapes changed (esp. satellites).
      - Never drop existing vault tables (non-destructive).
    """
    params = dict(operation.params)
    column_name = params.get("column_name")
    table_name = operation.target

    if not column_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_PARAM",
            error_message="column_name parameter is required for OPERATION_DROP_COLUMN",
        )

    if not table_name:
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_PERMANENT_ERROR,
            error_code="MISSING_TARGET",
            error_message="target (table_name) is required for OPERATION_DROP_COLUMN",
        )

    evidence_id = _make_evidence_id(operation)

    try:
        meta, hubs, links, sats = _discover_and_split(table_name)
        models_for_table = _load_models_for_table(table_name)

        created_models: List[str] = []

        # Ensure hub(s) (versioning logic already exists in NEW_HUB handler; reuse its internals by calling it)
        if hubs:
            # Call the existing hub handler logic (it is non-destructive and version-aware)
            hub_res = _handle_new_hub_for_vault(
                pb.Operation(
                    correlation_id=operation.correlation_id,
                    plan_id=operation.plan_id,
                    idempotency_key=f"{operation.idempotency_key}:hub",
                    layer=pb.LAYER_VAULT,
                    kind=pb.OPERATION_NEW_HUB,
                    target=table_name,
                    params=params,
                )
            )
            # NEW_HUB handler runs dbt itself; but we still track any generated model via metadata later.
            # We do not treat hub_res status as fatal unless permanent error.
            if hub_res.status == pb.OPERATION_STATUS_PERMANENT_ERROR:
                return hub_res

        # Ensure satellites (this is the key non-destructive behavior for DROP_COLUMN)
        if sats:
            sat_template = sats[0]
            created, already_present = _ensure_satellite_for_table(table_name, sat_template, models_for_table)
            created_models.extend(created)

        # Ensure links (version-aware NEW_LINK handler; reuse similarly)
        if links:
            link_res = _handle_new_link_for_vault(
                pb.Operation(
                    correlation_id=operation.correlation_id,
                    plan_id=operation.plan_id,
                    idempotency_key=f"{operation.idempotency_key}:link",
                    layer=pb.LAYER_VAULT,
                    kind=pb.OPERATION_NEW_LINK,
                    target=table_name,
                    params=params,
                )
            )
            if link_res.status == pb.OPERATION_STATUS_PERMANENT_ERROR:
                return link_res

        if created_models:
            run_dbt_models(created_models)

        write_metadata(
            {
                "layer": "vault",
                "target": table_name,
                "plan_id": operation.plan_id,
                "correlation_id": operation.correlation_id,
                "idempotency_key": operation.idempotency_key,
                "operation_kind": "DROP_COLUMN",
                "params": params,
                "evidence_id": evidence_id,
                "created_models": created_models,
                "note": "non-destructive; created side-by-side variants if needed",
            }
        )

        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_OK if created_models else pb.OPERATION_STATUS_ALREADY_APPLIED,
            error_code="",
            error_message="",
            evidence_snapshot_id=evidence_id,
        )

    except Exception as exc:
        msg = f"Vault DROP_COLUMN failed for {table_name}.{column_name}: {exc}"
        log.exception("[GRPC_SERVICE] %s", msg)
        return pb.OperationResult(
            correlation_id=operation.correlation_id,
            plan_id=operation.plan_id,
            idempotency_key=operation.idempotency_key,
            status=pb.OPERATION_STATUS_TRANSIENT_ERROR,
            error_code="DV_DROP_FAILED",
            error_message=msg,
            evidence_snapshot_id=evidence_id,
        )

def _compute_link_candidates(table_name: str, fk_filter: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Compute link candidates for a lake table using the same discovery + modelling
    logic as OPERATION_NEW_LINK, but without any side effects.

    Returns: list of link dicts, each expected to contain:
      - name: str
      - keys: List[str]  (e.g. [hub_key, fk_column])
    """
    meta, hubs, links, sats = _discover_and_split(table_name)

    candidates: List[Dict[str, Any]] = []
    for l in links:
        keys = list(l.get("keys") or [])
        if fk_filter and (len(keys) < 2 or keys[1] != fk_filter):
            continue
        candidates.append(l)

    return candidates

# ---------------------------------------------------------------------------
# Evidence introspection
# ---------------------------------------------------------------------------


def _introspect_vault_for_table(table_name: str) -> Dict[str, Any]:
    """
    Derive a simple logical view of the vault structures for a given table
    by inspecting dbt model JSON files.
    """
    models = _load_models_for_table(table_name)
    hub_names = [m["model_name"] for m in _models_by_type(models, "hub")]
    link_names = [m["model_name"] for m in _models_by_type(models, "link")]
    sat_names = [m["model_name"] for m in _models_by_type(models, "sat")]

    tables: List[Dict[str, Any]] = []
    for m in models:
        cols = list(m.get("columns") or [])
        tables.append({"name": m["model_name"], "columns": cols})

    vaults: List[Dict[str, Any]] = []
    if hub_names:
        vaults.append(
            {
                "hub": hub_names[0],
                "links": link_names,
                "satellites": sat_names,
            }
        )

    return {"vaults": vaults, "tables": tables}


class VaultHandlerService(pb_grpc.VaultHandlerServicer):
    """
    gRPC implementation for the Vault handler.

    Supports:
      - OPERATION_ADD_COLUMN  (extend satellites)
      - OPERATION_NEW_HUB     (non-destructive hub versioning)
      - OPERATION_NEW_LINK    (non-destructive link versioning)
    """

    def ApplyOperations(
        self,
        request: pb.OperationBatch,
        context: grpc.ServicerContext,
    ) -> pb.OperationBatchResult:
        results: List[pb.OperationResult] = []

        for op in request.operations:
            kind_name = pb.OperationKind.Name(op.kind)
            layer_name = pb.Layer.Name(op.layer)

            log.info(
                f"[GRPC_SERVICE] VaultHandler.ApplyOperations: plan_id={op.plan_id} correlation_id="
                f"{op.correlation_id} layer={layer_name} target={op.target} kind={kind_name} params={dict(op.params)}"
            )

            try:
                if op.layer != pb.LAYER_VAULT:
                    result = pb.OperationResult(
                        correlation_id=op.correlation_id,
                        plan_id=op.plan_id,
                        idempotency_key=op.idempotency_key,
                        status=pb.OPERATION_STATUS_ALREADY_APPLIED,
                        error_code="WRONG_LAYER",
                        error_message=f"Operation layer {layer_name} not handled by VaultHandler",
                    )
                elif op.kind == pb.OPERATION_ADD_COLUMN:
                    result = _handle_add_column_for_vault(op)
                elif op.kind == pb.OPERATION_NEW_HUB:
                    result = _handle_new_hub_for_vault(op)
                elif op.kind == pb.OPERATION_NEW_LINK:
                    result = _handle_new_link_for_vault(op)
                elif op.kind == pb.OPERATION_CHANGE_TYPE:
                    result = _handle_change_type_for_vault(op)
                elif op.kind == pb.OPERATION_DROP_COLUMN:
                    result = _handle_drop_column_for_vault(op)
                else:
                    msg = f"[GRPC_SERVICE] Operation kind {kind_name} not supported by VaultHandler"
                    log.error(msg)
                    result = pb.OperationResult(
                        correlation_id=op.correlation_id,
                        plan_id=op.plan_id,
                        idempotency_key=op.idempotency_key,
                        status=pb.OPERATION_STATUS_PERMANENT_ERROR,
                        error_code="UNSUPPORTED_OPERATION",
                        error_message=msg,
                    )

            except Exception as exc:
                # IMPORTANT: never let exceptions escape the gRPC handler, otherwise
                # SEF sees StatusCode.UNKNOWN/UNAVAILABLE and cannot distinguish transient
                # dependency issues from permanent failures.
                msg = f"[GRPC_SERVICE] Exception calling application: {exc}"
                log.exception(msg)

                # Mark as transient: SEF should retry.
                result = pb.OperationResult(
                    correlation_id=op.correlation_id,
                    plan_id=op.plan_id,
                    idempotency_key=op.idempotency_key,
                    status=pb.OPERATION_STATUS_TRANSIENT_ERROR,
                    error_code="DV_TRANSIENT_ERROR",
                    error_message=msg,
                    evidence_snapshot_id=_make_evidence_id(op),
                )

                # Best-effort: record the failure as metadata (do not raise on failure)
                try:
                    write_metadata(
                        {
                            "layer": "vault",
                            "target": op.target,
                            "plan_id": op.plan_id,
                            "correlation_id": op.correlation_id,
                            "idempotency_key": op.idempotency_key,
                            "operation_kind": kind_name,
                            "params": dict(op.params),
                            "evidence_id": result.evidence_snapshot_id,
                            "source": "vault_handler_grpc",
                            "note": f"transient_error: {exc}",
                        }
                    )
                except Exception:
                    pass

            results.append(result)

        return pb.OperationBatchResult(results=results)

    def IntrospectEvidence(
        self,
        request: pb.EvidenceRequest,
        context: grpc.ServicerContext,
    ) -> pb.EvidenceResponse:
        table_name = request.dataset_id  # assuming dataset_id == lake table name

        log.info(
            f"[GRPC_SERVICE] VaultHandler.IntrospectEvidence: plan_id={request.plan_id} correlation_id="
            f"{request.correlation_id} dataset_id={table_name}")

        info = _introspect_vault_for_table(table_name)
        vaults = info["vaults"]
        tables = info["tables"]

        pb_tables: List[pb.TableDescriptor] = []
        for t in tables:
            attrs = [
                pb.AttributeDescriptor(
                    name=c,
                    logical_type="",
                    physical_type="",
                    nullable=True,
                )
                for c in t.get("columns", [])
            ]
            pb_tables.append(
                pb.TableDescriptor(
                    name=t["name"],
                    attributes=attrs,
                )
            )

        pb_vaults: List[pb.VaultDescriptor] = []
        for v in vaults:
            pb_vaults.append(
                pb.VaultDescriptor(
                    hub=v["hub"],
                    links=v["links"],
                    satellites=v["satellites"],
                )
            )

        raw = {
            "dataset_id": table_name,
            "plan_id": request.plan_id,
            "vaults": vaults,
            "tables": tables,
        }

        return pb.EvidenceResponse(
            correlation_id=request.correlation_id,
            plan_id=request.plan_id,
            tables=pb_tables,
            vault_structures=pb_vaults,
            raw_evidence_json=json.dumps(raw),
        )

    def ProbeLinkCandidates(
            self,
            request: pb.LinkProbeRequest,
            context: grpc.ServicerContext,
    ) -> pb.LinkProbeResponse:
        table_name = request.table_name
        fk_filter = request.fk_filter or None

        log.info(
            f"[GRPC_SERVICE] VaultHandler.ProbeLinkCandidates: plan_id={request.plan_id} "
            f"correlation_id={request.correlation_id} table_name={table_name} fk_filter={fk_filter}"
        )

        if not table_name:
            return pb.LinkProbeResponse(
                correlation_id=request.correlation_id,
                plan_id=request.plan_id,
                candidates=[],
                error_code="MISSING_TABLE_NAME",
                error_message="table_name is required",
            )

        try:
            candidates = _compute_link_candidates(table_name, fk_filter=fk_filter)
            pb_candidates = [
                pb.LinkCandidate(
                    name=str(c.get("name") or ""),
                    keys=[str(x) for x in (c.get("keys") or [])],
                )
                for c in candidates
            ]
            return pb.LinkProbeResponse(
                correlation_id=request.correlation_id,
                plan_id=request.plan_id,
                candidates=pb_candidates,
                error_code="",
                error_message="",
            )
        except Exception as exc:
            # Important: discovery can fail transiently if the lake table isn't created yet.
            msg = f"ProbeLinkCandidates failed for table {table_name!r}: {exc}"
            log.warning("[GRPC_SERVICE] %s", msg)
            return pb.LinkProbeResponse(
                correlation_id=request.correlation_id,
                plan_id=request.plan_id,
                candidates=[],
                error_code="DV_DISCOVERY_FAILED",
                error_message=msg,
            )


def serve(stop_event: "threading.Event | None" = None) -> None:
    """
    Start the Vault gRPC server.

    If stop_event is None, this will block with server.wait_for_termination()
    and can be used as a standalone entrypoint.

    If stop_event is provided, this function will return when the event is set,
    stopping the server gracefully. This is suitable for running in a
    background thread from main.py.
    """
    port = int(os.getenv("VAULT_HANDLER_GRPC_PORT", "50052"))
    max_workers = int(os.getenv("VAULT_HANDLER_GRPC_MAX_WORKERS", "10"))

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    pb_grpc.add_VaultHandlerServicer_to_server(VaultHandlerService(), server)

    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    log.info("Starting Vault gRPC handler on %s", listen_addr)

    server.start()
    log.info("Vault gRPC handler started; waiting for requests from SEF core.")

    if stop_event is None:
        # Standalone mode
        server.wait_for_termination()
    else:
        # Cooperative shutdown mode
        try:
            while not stop_event.is_set():
                time.sleep(0.5)
        finally:
            log.info("Vault gRPC stop_event set, stopping server...")
            server.stop(grace=5)
            log.info("Vault gRPC server stopped.")


if __name__ == "__main__":
    serve()
