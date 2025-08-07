import re

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
    cols = list(df.columns)
    key_candidates = [c for c in cols if any(x in c.lower() for x in KEY_SUFFIXES)]
    base = _singularize(table_name)
    hub_key = None
    for c in key_candidates:
        if _normalize(c).startswith(base):
            hub_key = c
            break
    if not hub_key and key_candidates:
        hub_key = key_candidates[0]
    foreign_keys = [c for c in key_candidates if c != hub_key]
    attributes = [c for c in cols if c not in key_candidates]
    return {
        "columns": cols,
        "business_keys": [hub_key] + foreign_keys if hub_key else foreign_keys,
        "attributes": attributes,
        "hub_key": hub_key,
        "foreign_keys": foreign_keys,
    }


def split_datavault(table_name, meta):
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

    for fk in foreign_keys:
        fk_base = _singularize(_strip_key_suffix(fk))
        link_name = f"link_{base}_{fk_base}"
        links.append({"name": link_name, "keys": [hub_key, fk]})

    return hubs, links, satellites

def get_model_type(meta):
    cols = meta['columns']
    bk = meta['business_keys']
    n_cols = len(cols)
    n_bk = len(bk)
    # Heuristic:
    # Link: >1 BK and (almost) all columns are BKs
    # Hub:  1 BK (possibly with load_datetime)
    # Sat:  Anything else

    # If only one BK and (almost) all columns are BKs or audit fields, it's a Hub
    if n_bk == 1:
        return "hub"
    # Link: more than 1 BK, and all columns are BKs or standard audit columns
    audit_cols = {"load_datetime", "created_at", "modified_at", "record_source"}
    non_bk = [c for c in cols if c not in bk and c.lower() not in audit_cols]
    if n_bk > 1 and len(non_bk) == 0:
        return "link"
    # If has at least one BK and at least one non-key column, it's a Satellite
    if n_bk >= 1 and len(non_bk) > 0:
        return "sat"
    # fallback (treat as sat)
    return "sat"

