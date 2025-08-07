import re
from logger import log

KEY_SUFFIXES = ["id", "nr", "key", "number"]


def _normalize(name: str) -> str:
    """Normalize a column name for comparison."""
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _strip_key_suffix(col: str) -> str:
    """Remove standard key suffixes (id, code, key, nr, number)."""
    col_lower = col.lower()
    for suf in KEY_SUFFIXES:
        if col_lower.endswith("_" + suf):
            return col_lower[: -(len(suf) + 1)]
        if col_lower.endswith(suf):
            return col_lower[: -len(suf)]
    return col_lower


def _singularize(name: str) -> str:
    """Very small helper to singularize table names."""
    n = name.lower()
    if n.endswith("ies"):
        return n[:-3] + "y"
    if n.endswith("s") and not n.endswith("ss"):
        return n[:-1]
    return n


def extract_metadata(table_name, df):
    log.debug(f"Extracting metadata for table '{table_name}' with columns {list(df.columns)}")
    cols = list(df.columns)
    if not cols:
        log.error(f"No columns found for table '{table_name}'")
    key_candidates = [c for c in cols if any(x in c.lower() for x in KEY_SUFFIXES)]
    base = _singularize(table_name)
    hub_key = None
    for c in key_candidates:
        if _normalize(c).startswith(base):
            hub_key = c
            break
    if not hub_key and key_candidates:
        hub_key = key_candidates[0]
        log.warning(f"Hub key heuristics fallback for table '{table_name}' -> '{hub_key}'")
    if hub_key:
        log.info(f"Selected hub key '{hub_key}' for table '{table_name}'")
    else:
        log.warning(f"No hub key detected for table '{table_name}'")
    foreign_keys = [c for c in key_candidates if c != hub_key]
    attributes = [c for c in cols if c not in key_candidates]
    metadata = {
        "columns": cols,
        "business_keys": [hub_key] + foreign_keys if hub_key else foreign_keys,
        "attributes": attributes,
        "hub_key": hub_key,
        "foreign_keys": foreign_keys,
    }
    if not metadata["business_keys"]:
        log.critical(f"No business keys determined for table '{table_name}'")
    log.debug(f"Metadata for '{table_name}': {metadata}")
    return metadata


def split_datavault(table_name, meta):
    log.debug(f"Splitting table '{table_name}' with metadata {meta}")
    hub_key = meta.get("hub_key")
    foreign_keys = meta.get("foreign_keys", [])
    attributes = meta.get("attributes", [])

    base = _singularize(table_name)

    hubs = []
    links = []
    satellites = []

    if hub_key:
        hubs.append({"name": f"hub_{base}", "key": [hub_key]})
        if attributes:
            satellites.append({
                "name": f"sat_{base}",
                "key": [hub_key],
                "attributes": attributes,
            })
    else:
        log.warning(f"No hub key found for table '{table_name}' during split")

    for fk in foreign_keys:
        fk_base = _singularize(_strip_key_suffix(fk))
        link_name = f"link_{base}_{fk_base}"
        links.append({"name": link_name, "keys": [hub_key, fk]})

    log.info(
        f"Split results for '{table_name}': {len(hubs)} hubs, {len(links)} links, {len(satellites)} satellites"
    )
    return hubs, links, satellites

def get_model_type(meta):
    cols = meta['columns']
    bk = meta['business_keys']
    n_cols = len(cols)
    n_bk = len(bk)
    log.debug(f"Determining model type with {n_cols} columns and {n_bk} business keys")
    if n_bk == 0:
        log.warning("No business keys found; defaulting model type to 'sat'")
    # Heuristic:
    # Link: >1 BK and (almost) all columns are BKs
    # Hub:  1 BK (possibly with load_datetime)
    # Sat:  Anything else

    # If only one BK and (almost) all columns are BKs or audit fields, it's a Hub
    if n_bk == 1:
        model = "hub"
    else:
        audit_cols = {"load_datetime", "created_at", "modified_at", "record_source"}
        non_bk = [c for c in cols if c not in bk and c.lower() not in audit_cols]
        if n_bk > 1 and len(non_bk) == 0:
            model = "link"
        elif n_bk >= 1 and len(non_bk) > 0:
            model = "sat"
        else:
            model = "sat"
            log.error("Unable to classify model type clearly; defaulting to 'sat'")
    log.info(f"Model type determined: {model}")
    return model