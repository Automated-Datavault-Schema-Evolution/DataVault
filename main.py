import json
import os
import subprocess
import threading

import pandas as pd
import yaml
from jinja2 import Template
from logger import log
from pyhive import hive

from cdc_kafka_producer import cdc_producer_insert_only, produce_tables_once
from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA, DBT_MODELS_JSON_DIR, THRIFT_HOST, THRIFT_PORT, DBT_MODELS_SQL_DIR,
    KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID, STAGING_SCHEMA, RAW_VAULT_SCHEMA,
)
from dv_modeller import extract_metadata, split_datavault
from meta_store import write_lineage, write_metadata
from utils.bronze_ingestor import start_bronze_writer, truncate_bronze_table, ensure_bronze_table_exists
from utils.helper_spark import get_spark_session, ensure_spark_warehouse_dir
from utils.performance_logger import PerfListener, log_progress_periodically
from utils.schema_helpers import bronze_target_columns

_DBT_LOCK = threading.Lock()  # serialize dbt runs during streaming

def write_text_if_changed(path: str, content: str) -> bool:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    try:
        with open(path, "r", encoding="utf-8") as f:
            if f.read() == content:
                return False
    except FileNotFoundError:
        pass
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return True


def run_dbt_models(models):
    """Run dbt for the specified models."""
    if not models:
        return
    ensure_profiles_dir()
    cmd = [
              "dbt",
              "run",
              "--profiles-dir",
              DBT_PROFILES_DIR,
              "--select",
          ] + sorted(models)
    log.info(f"[DBT] Running: {cmd}")
    # use check=False to keep app running even if some models fail
    with _DBT_LOCK:
        subprocess.run(cmd, check=False)


def _resolve_thrift(target_cfg):
    """Resolve Hive Thrift connection parameters with env taking precedence."""
    env_host = os.environ.get("THRIFT_HOST")
    env_port = os.environ.get("THRIFT_PORT")
    host = env_host or target_cfg.get("host") or THRIFT_HOST
    port = int(env_port or target_cfg.get("port") or THRIFT_PORT)
    user = target_cfg.get("user")
    log.debug(f"Using Hive Thrift server host={host}, port={port}")
    return host, port, user


def discover_lake():
    """Return available lake tables and a loader function."""
    log.debug(f"LAKE_TYPE: {LAKE_TYPE}")
    if LAKE_TYPE == "parquet":
        tables = [f[:-8] for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")]

        def load_table(t):
            path = os.path.join(PARQUET_PATH, t + ".parquet")
            # read just schema if pyarrow is available; otherwise use head(0)
            try:
                import pyarrow.parquet as pq
                pf = pq.ParquetFile(path)
                cols = list(pf.schema.names)
                return pd.DataFrame(columns=cols)
            except Exception:
                return pd.read_parquet(path).head(0)
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
            q = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
                sql.Identifier(RDBMS_SCHEMA), sql.Identifier(t)
            )
            cur.execute(q)
            cols = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            # return a dataframe-like schema descriptor; dv_modeller.extract_metadata handles it
            return pd.DataFrame(columns=cols)

        return tables, load_table
    else:
        raise ValueError(f"Unsupported LAKE_TYPE: {LAKE_TYPE}")

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
    if "{{" in schema:
        schema = Template(schema).render(env_var=lambda name, default=None: os.getenv(name, default))
    host, port, user = _resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user)
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
    cursor.close()
    conn.close()
    log.info(f"[DB] Ensured database/schema '{schema}' exists")


def write_sql_model_file(model_name, table_name, model_type, meta):
    """Create/update a dbt SQL model file based on JSON metadata (idempotent)."""
    os.makedirs(DBT_MODELS_SQL_DIR, exist_ok=True)
    file_path = os.path.join(DBT_MODELS_SQL_DIR, f"{model_name}.sql")
    business_keys = meta.get("business_keys", [])
    attributes = meta.get("attributes", [])
    columns = business_keys + attributes

    incremental_conf = (
        "{{ config(\n"
        "    materialized='incremental',\n"
        "    incremental_strategy='insert_overwrite',\n"
        "    on_schema_change='sync_all_columns'\n"
        ") }}\n"
    )
    lines = [incremental_conf, "select"]
    for col in columns:
        lines.append(f"    {col},")
    lines.append("    current_timestamp() as load_datetime,")
    lines.append(f"    '{table_name}' as record_source")
    lines.append(f"from {{{{ source('staging', '{table_name}') }}}}")
    if model_type in {"hub", "link"} and business_keys:
        lines.append(f"group by {', '.join(business_keys)}")

    content = "\n".join(lines) + "\n"
    wrote = write_text_if_changed(file_path, content)
    if wrote:
        log.debug(f"[DBT] Wrote SQL model {model_name}.sql")
    return wrote


def write_json_model_file(model_name, table_name, model_type, meta):
    """Persist model metadata as JSON for dbt-spark (idempotent) and sync SQL."""
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
    json_txt = json.dumps(model_def, indent=2) + "\n"
    wrote_json = write_text_if_changed(file_path, json_txt)
    wrote_sql = write_sql_model_file(model_name, table_name, model_type, model_def)
    if wrote_json or wrote_sql:
        write_metadata(model_def)
        write_lineage(
            {
                "source_table": table_name,
                "target_model": model_name,
                "model_type": model_type,
                "business_keys": model_def["business_keys"],
                "attributes": model_def["attributes"],
                "columns": model_def["columns"],
            }
        )
        log.info(f"[GEN] Generated/updated DBT JSON model for {model_name} (from lake table {table_name})")


def generate_schema_yml(table_names, output_path="models/schema.yml"):
    lines = []
    lines.append("version: 2")
    lines.append("")
    lines.append("sources:")
    lines.append("  - name: staging")
    lines.append('    schema: "{{ env_var(\'STAGING_SCHEMA\', \'bronze\') }}"')
    lines.append("    tables:")
    for t in table_names:
        lines.append(f"      - name: {t}")
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
    host, port, user = _resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user)
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
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)  # earlist for first run, then checkpoint
        .option("groupIdPrefix", KAFKA_GROUP_ID)
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
    from pyspark.sql.types import StructField, StringType, StructType
    fields = [StructField(k, StringType(), True) for k in payload_json.keys()]
    return StructType(fields)


def streaming_dv_consumer_and_dbt(models_to_run):
    spark = get_spark_session("DataVault_Streaming_Consumer")
    spark.streams.addListener(PerfListener())
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

    for r in table_rows:
        if r["table"] is not None:
            log.info("[KAFKA] table=%s, backlog_messages=%s", r["table"], r["count"])

    table_names = [row["table"] for row in table_rows if row["table"] is not None]

    queries = []
    ensured_tables = []

    table_to_models = get_existing_model_tables()
    for table_name in table_names:
        schema = infer_schema_from_cdc_event(spark, table_name)
        if not schema:
            log.info(f"[DBT Model] Skipping {table_name}: could not infer schema")
            continue

        if ensure_bronze_table_exists(spark, table_name, schema):
            ensured_tables.append(table_name)

        df_stream = get_kafka_stream(spark, table_name, schema)

        # On each micro-batch: run only the model for this table and then truncate bronze.<table>
        # def after_write(_epoch_id: int, _tbl=table_name):
        #     models = table_to_models.get(_tbl, [])
        #     if models:
        #         run_dbt_models(models)
        #     # truncate_bronze_table(spark, _tbl)
        def after_write(_epoch_id: int, _tbl=table_name):
            # 1) show current bronze count for this table (post-append)
            try:
                bronze_cnt = spark.table(f"{STAGING_SCHEMA}.{_tbl}").count()
                log.info("[BRONZE][%s] post-append table count = %s", _tbl, bronze_cnt)
            except Exception as e:
                log.warning("[BRONZE][%s] Could not read bronze table for count: %s", _tbl, e)

            # 2) run related dbt models for this lake table
            models = table_to_models.get(_tbl, [])
            if models:
                run_dbt_models(models)

                # 3) after dbt: log raw_vault row counts per model
                for m in models:

                    try:
                        fq = f"{RAW_VAULT_SCHEMA}.{m}"
                        if spark.catalog.tableExists(fq):
                            cnt = spark.table(fq).count()
                            log.info("[RAW_VAULT][%s] row_count = %s", fq, cnt)
                        else:
                            log.info("[RAW_VAULT][%s] not visible to this Spark session (yet)", fq)
                    except Exception as e:
                        log.debug("[RAW_VAULT][%s] count skipped (likely path/catalog mismatch): %s", fq, e)

        q = start_bronze_writer(spark, table_name, df_stream, on_after_write=after_write)
        if q:
            queries.append((table_name, q))
        log_progress_periodically(q)
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

    # if models_to_run:
    #     run_dbt_models(models_to_run)
    # Only run the initial dbt set if at least one Bronze table is present
    if models_to_run and ensured_tables:
        run_dbt_models(models_to_run)
    else:
        if models_to_run:
            log.info("[DBT] Skipping initial run; Bronze not ready yet. Models will run after first micro-batches.")

    log.info("Streaming ingestion to bronze is running. Press Ctrl+C to stop.")
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for _tbl, q in queries:
            q.stop()
        log.info("[BRONZE] Stopped streaming queries")

def bootstrap_bronze(lake_tables, load_table):
    from pyspark.sql.types import StructType, StructField, StringType
    spark = get_spark_session("DataVault_Bootstrap")
    spark.streams.addListener(PerfListener())

    for t in lake_tables:
        # Get column names from the lake; make a simple all-STRING schema for the empty Bronze
        # df_cols = list(load_table(t).columns)  # returns a pandas df with just columns for RDBMS/parquet loaders
        df_cols = bronze_target_columns(spark, t)
        if not df_cols:
            continue
        schema = StructType([StructField(c, StringType(), True) for c in df_cols])
        try:
            ensure_bronze_table_exists(spark, t, schema)
        except Exception as e:
            log.warning(f"[Bootstrap] Could not precreate bronze.{t}: {e}")

def main():
    os.makedirs(DBT_MODELS_JSON_DIR, exist_ok=True)
    ensure_spark_warehouse_dir()
    ensure_profiles_dir()
    ensure_database_schema()

    lake_tables, load_table = discover_lake()

    bootstrap_bronze(lake_tables, load_table)

    generate_schema_yml(lake_tables)
    existing_models = get_existing_model_tables()
    vault_tables = get_raw_vault_tables()

    # 1) Determine which tables are new (no model yet) and which vault objects are missing
    tables_without_models = [t for t in lake_tables if t not in existing_models]
    models_to_run = set()
    tables_needing_initial_load = set()

    if tables_without_models:
        # Generate models for new tables
        new_models = ensure_dbt_models_for_lake(tables_without_models, load_table)
        models_to_run.update(new_models)
        # all new tables will need an initial full load after objects are created
        tables_needing_initial_load.update(tables_without_models)

    # If any model exists but the physical table is missing in the vault, we must create it
    for table, model_names in existing_models.items():
        for m in model_names:
            if m not in vault_tables:
                models_to_run.add(m)
                # this lake table is missing at least one DV object -> full load needed
                tables_needing_initial_load.add(table)

    if models_to_run:
        log.info(f"[DBT] Will run for: {sorted(models_to_run)}")
    else:
        log.info("NO new Data Vault tables to create, all up to date")

    # 2) If we have models to create, do that first
    if models_to_run:
        run_dbt_models(sorted(models_to_run))

    # 3) INITIAL FULL LOAD for any lake tables whose DV objects were just created
    if tables_needing_initial_load:
        log.info(f"[INITIAL LOAD] Producing full load for tables: {sorted(tables_needing_initial_load)}]")
        # Produce once: this pushes all historical rows (by table) to Kafka
        produce_tables_once(sorted(tables_needing_initial_load))
        log.info("[INITIAL LOAD] Full load events produced")

    # One-shot initial load so the writer has data to land and dbt can build from Bronze
    try:
        produce_tables_once(lake_tables)
    except Exception as e:
        log.warning(f"[Bootstrap] Initial produce failed (continuing with streaming CDC): {e}")

    # 4) Start CDC producer and streaming consumer
    stop_event = threading.Event()
    cdc_thread = threading.Thread(target=cdc_producer_insert_only, daemon=True)
    cdc_thread.start()

    try:
        streaming_dv_consumer_and_dbt(models_to_run)
    except KeyboardInterrupt:
        log.info("Shutting down CDC producer.")
        stop_event.set()
        cdc_thread.join()
        log.info("All done.")

if __name__ == "__main__":
    main()
