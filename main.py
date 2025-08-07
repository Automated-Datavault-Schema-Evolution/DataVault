import os
import sys
import json
import time
import threading
import pandas as pd
import yaml
from pyhive import hive
from cdc_kafka_producer import cdc_producer_insert_only
from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA, DBT_MODELS_JSON_DIR, THRIFT_HOST, THRIFT_PORT, THRIFT_AUTH,
)
from dv_modeller import extract_metadata, split_datavault
from meta_store import write_lineage, write_metadata
from utils import get_spark_session
from logger import log

# Directory where JSON model descriptions will be written
# (configured via DBT_MODELS_JSON_DIR in config.py)
WATERMARK_FILE = "cdc_watermarks.json"

def _resolve_thrift(target_cfg):
    """Resolve Hive Thrift connection parameters with env taking precedence."""
    env_host = os.environ.get("THRIFT_HOST")
    env_port = os.environ.get("THRIFT_PORT")
    env_auth = os.environ.get("THRIFT_AUTH")
    host = env_host or target_cfg.get("host") or THRIFT_HOST
    port = int(env_port or target_cfg.get("port") or THRIFT_PORT)
    auth = env_auth or THRIFT_AUTH
    user = target_cfg.get("user")
    log.debug(f"Using Hive Thrift server host={host}, port={port}, auth={auth}")
    return host, port, user, auth

def discover_lake():
    """Return available lake tables and a loader function."""
    log.debug(f"LAKE_TYPE: {LAKE_TYPE}")
    if LAKE_TYPE == "parquet":
        tables = [f[:-8] for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")]
        load_table = lambda t: pd.read_parquet(os.path.join(PARQUET_PATH, t + ".parquet"), nrows=1)
    elif LAKE_TYPE == "rdbms":
        import psycopg2
        from psycopg2 import sql
        conn = psycopg2.connect(
            host=RDBMS_HOST, port=RDBMS_PORT,
            dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
        )
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = %s",
            (RDBMS_SCHEMA, "BASE TABLE")
        )
        tables = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()

        def load_table(t):
            conn = psycopg2.connect(
                host=RDBMS_HOST, port=RDBMS_PORT,
                dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
            )
            cur = conn.cursor()
            q = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(sql.Identifier(RDBMS_SCHEMA), sql.Identifier(t))
            cur.execute(q)
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return pd.DataFrame(columns=colnames)
    else:
        log.critical(f"Unknown LAKE_TYPE '{LAKE_TYPE}'")
        raise ValueError("Unknown LAKE_TYPE")
    log.info(f"Discovered {len(tables)} table(s) in {LAKE_TYPE} lake")
    return tables, load_table

def ensure_database_schema():
    """Create target Spark database/schema if it does not exist."""
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        return
    with open(profiles_yml_path, "r") as f:
        profiles = yaml.safe_load(f) or {}
    default_profile = profiles.get("default", {})
    target = default_profile.get("target")
    outputs = default_profile.get("outputs", {})
    target_cfg = outputs.get(target, {})
    schema = target_cfg.get("schema") or target_cfg.get("database")
    if not schema:
        return
    host, port, user, auth = _resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user, auth=auth)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
    cursor.close()
    conn.close()
    log.info(f"[DB] Ensured database/schema '{schema}' exists")

def write_json_model_file(model_name, table_name, model_type, meta):
    """Persist model metadata as JSON for dbt-spark."""
    os.makedirs(DBT_MODELS_JSON_DIR, exist_ok=True)
    file_path = os.path.join(DBT_MODELS_JSON_DIR, f"{model_name}.json")
    model_def = {
        "model_name": model_name,
        "table_name": table_name,
        "model_type": model_type,
        "business_keys": meta.get("business_keys", []),
        "attributes": meta.get("attributes", []),
        "columns": meta.get("columns", []),
    }
    with open(file_path, "w") as file:
        json.dump(model_def, file, indent=2)
    write_metadata(model_def)
    log.info(f"[GEN] Generated DBT JSON model for {model_name} (from lake table {table_name})")

def generate_schema_yml(table_names, output_path="models/schema.yml"):
    lines = []
    lines.append("version: 2")
    lines.append("")
    lines.append("sources:")
    lines.append("  - name: staging")
    lines.append("    tables:")
    for t in table_names:
        lines.append(f"      - name: {t}")
    with open(output_path, "w") as f:
        f.write("\n".join(lines))

def ensure_dbt_models_for_lake(tables, load_table):
    # Generate JSON model metadata for each lake table
    new_models = []
    for table in tables:
        df_schema = load_table(table)
        meta = extract_metadata(table, df_schema)
        hubs, links, sats = split_datavault(table, meta)

        # Generate Hubs
        for hub in hubs:
            model_name = hub["name"]
            bk = hub["key"]
            write_json_model_file(
                model_name, table, "hub", {"business_keys": bk, "attributes": [], "columns": bk}
            )
            new_models.append(model_name)

        # Generate Links
        for link in links:
            model_name = link["name"]
            keys = link["keys"]
            write_json_model_file(
                model_name, table, "link", {"business_keys": keys, "attributes": [], "columns": keys}
            )
            new_models.append(model_name)

        # Generate Satellites
        for sat in sats:
            model_name = sat["name"]
            keys = sat["key"]
            atts = sat["attributes"]
            write_json_model_file(
                model_name, table, "sat",
                {"business_keys": keys, "attributes": atts, "columns": keys + atts},
            )
            new_models.append(model_name)
    return new_models

def get_existing_model_tables():
    """Return a mapping of lake tables to their generated model names."""
    table_models = {}
    if not os.path.exists(DBT_MODELS_JSON_DIR):
        return table_models
    for fname in os.listdir(DBT_MODELS_JSON_DIR):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(DBT_MODELS_JSON_DIR, fname), "r") as f:
            data = json.load(f)
        table_models.setdefault(data.get("table_name"), []).append(data.get("model_name"))
    return table_models

def get_raw_vault_tables():
    """List existing tables in the raw vault."""
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        return set()
    with open(profiles_yml_path, "r") as f:
        profiles = yaml.safe_load(f) or {}
    default_profile = profiles.get("default", {})
    target = default_profile.get("target")
    outputs = default_profile.get("outputs", {})
    target_cfg = outputs.get(target, {})
    schema = target_cfg.get("schema") or target_cfg.get("database")
    if not schema:
        return set()
    host, port, user, auth = _resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user, auth=auth)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return set(tables)

def ensure_profiles_dir():
    """
    Ensure dbt profiles directory exists in the project and create a default profiles.yml if needed.
    """
    if not os.path.exists(DBT_PROFILES_DIR):
        os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
        log.info(f"[INFO] Created dbt profiles directory: {DBT_PROFILES_DIR}")

    # (Optional) Create a default profiles.yml if not present
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        with open(profiles_yml_path, "w") as f:
            f.write("# Insert your dbt profile config here\n")
        log.info(f"[INFO] Created empty profiles.yml at: {profiles_yml_path}")


# Spark consumer (Streaming + model generation)
def get_kafka_stream(spark, table_name, schema):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    df_data = df_table.select(from_json(col("json.payload"), schema).alias("data")).select("data.*")
    return df_data

def infer_schema_from_cdc_event(spark, table_name):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    sample = df_table.limit(1).collect()
    if not sample:
        return None
    payload_json = json.loads(sample[0]["json"]["payload"])
    fields = [StructField(k, StringType(), True) for k in payload_json.keys()]
    return StructType(fields)


def streaming_dv_consumer_and_dbt():
    spark = get_spark_session("DataVault_Streaming_Consumer")
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    table_rows = df_json.groupBy(col("json.table")).count().collect()
    table_names = [row["table"] for row in table_rows if row["table"] is not None]

    for table_name in table_names:
        schema = infer_schema_from_cdc_event(spark, table_name)
        if not schema:
            log.info(f"[DBT Model] Skipping {table_name}: could not infer schema")
            continue
        df_stream = get_kafka_stream(spark, table_name, schema)
        meta = extract_metadata(table_name, df_stream)
        hubs, links, sats = split_datavault(table_name, meta)

        for hub in hubs:
            write_json_model_file(
                hub["name"], table_name, "hub",
                {"business_keys": hub["key"], "attributes": [], "columns": hub["key"]},
            )

        for link in links:
            write_json_model_file(
                link["name"], table_name, "link",
                {"business_keys": link["keys"], "attributes": [], "columns": link["keys"]},
            )

        for sat in sats:
            write_json_model_file(
                sat["name"], table_name, "sat",
                {
                    "business_keys": sat["key"],
                    "attributes": sat["attributes"],
                    "columns": sat["key"] + sat["attributes"],
                },
            )
        log.info(f"[DBT Model] Generated metadata for: {table_name}")

    log.info("[DBT] Running all models...")
    ensure_profiles_dir()
    ensure_database_schema()
    exit_code = os.system(f"dbt run --profiles-dir {DBT_PROFILES_DIR}")
    if exit_code != 0:
        log.critical(f"dbt run failed with exit code {exit_code}")
        raise RuntimeError(f"dbt run failed with exit code {exit_code}")
    log.info("dbt run completed successfully")



def main():
    os.makedirs(DBT_MODELS_JSON_DIR, exist_ok=True)
    ensure_profiles_dir()
    ensure_database_schema()

    lake_tables, load_table = discover_lake()
    generate_schema_yml(lake_tables)
    existing_models = get_existing_model_tables()
    vault_tables = get_raw_vault_tables()

    tables_without_models = [t for t in lake_tables if t not in existing_models]
    models_to_run = set()

    if tables_without_models:
        new_models = ensure_dbt_models_for_lake(tables_without_models, load_table)
        models_to_run.update(new_models)

    for table, model_names in existing_models.items():
        for m in model_names:
            if m not in vault_tables:
                models_to_run.add(m)

    if models_to_run:
        log.info(f"[DBT] Running dbt for: {sorted(models_to_run)}")
        os.system(
            f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select {' '.join(models_to_run)}"
        )
    else:
        log.info("NO new Data Vault tables to create, all up to date")

    if tables_without_models:
        from cdc_kafka_producer import produce_tables_once
        produce_tables_once(tables_without_models)
    stop_event = threading.Event()
    cdc_thread = threading.Thread(target=cdc_producer_insert_only, daemon=True)
    cdc_thread.start()

    try:
        streaming_dv_consumer_and_dbt()
    except KeyboardInterrupt:
        log.info("Shutting down CDC producer...")
        stop_event.set()
        cdc_thread.join()
        log.info("All done.")

if __name__ == "__main__":
    main()
