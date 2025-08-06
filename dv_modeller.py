from itertools import combinations

def extract_metadata(df):
    cols = list(df.columns)
    business_keys = [c for c in cols if any(x in c.lower() for x in ["id", "nr", "key", "code", "number"])]
    attributes = [c for c in cols if c not in business_keys]
    return {
        'columns': cols,
        'business_keys': business_keys,
        'attributes': attributes
    }

def split_datavault(table_name, columns):
    business_keys = [c for c in columns if any(x in c.lower() for x in ["id", "nr", "key", "code", "number"])]
    attributes = [c for c in columns if c not in business_keys]

    hubs = []
    links = []
    satellites = []

    # Hubs
    for bk in business_keys:
        hubs.append({
            "name": f"hub_{table_name}",
            "key": [bk]
        })

    # Links (all 2-combinations)
    for combo in combinations(business_keys, 2):
        links.append({
            "name": f"link_{'_'.join([bk.lower() for bk in combo])}",
            "keys": list(combo)
        })

    # Satellites for each hub
    for hub in hubs:
        sat_atts = [a for a in attributes if a != hub["key"][0]]
        if sat_atts:
            satellites.append({
                "name": f"sat_{hub['key'][0].lower()}",
                "key": hub["key"],
                "attributes": sat_atts
            })
    # Satellites for each link
    for link in links:
        sat_atts = attributes
        if sat_atts:
            satellites.append({
                "name": f"sat_{'_'.join([k.lower() for k in link['keys']])}",
                "key": link["keys"],
                "attributes": sat_atts
            })

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

def get_builder_code(table_name, model_type, meta):
    bk = meta.get('business_keys', [])
    # Hub example
    if model_type == "hub" and bk:
        select_cols = ", ".join([f'"{b}"' for b in bk])
        hash_expr = ", ".join([f'df["{b}"].cast("string")' for b in bk])
        return (
            "out_df = df.select({cols}) \\\n"
            "    .distinct() \\\n"
            "    .withColumn(\"load_datetime\", F.current_timestamp()) \\\n"
            "    .withColumn(\"{table}_hashkey\", F.md5(F.concat_ws(\"||\", {hash_expr})))"
        ).format(
            cols=select_cols,
            table=table_name,
            hash_expr=hash_expr
        )
    # Link example
    elif model_type == "link" and len(bk) > 1:
        select_cols = ", ".join([f'"{b}"' for b in bk])
        hash_expr = ", ".join([f'df["{b}"].cast("string")' for b in bk])
        return (
            "out_df = df.select({cols}) \\\n"
            "    .distinct() \\\n"
            "    .withColumn(\"load_datetime\", F.current_timestamp()) \\\n"
            "    .withColumn(\"{table}_link_hashkey\", F.md5(F.concat_ws(\"||\", {hash_expr})))"
        ).format(
            cols=select_cols,
            table=table_name,
            hash_expr=hash_expr
        )
    # Satellite example (all columns except business keys)
    elif model_type == "sat":
        non_bk_cols = [c for c in meta.get('columns', []) if c not in bk]
        select_cols = ", ".join([f'"{c}"' for c in non_bk_cols])
        return (
            "out_df = df.select({cols}) \\\n"
            "    .withColumn(\"load_datetime\", F.current_timestamp())"
        ).format(cols=select_cols)
    else:
        return "out_df = df"