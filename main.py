import atexit
import fcntl
import json
import os
import signal
import subprocess
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import yaml
from jinja2 import Template
from logger import log
from pyhive import hive

from cdc_kafka_producer import cdc_producer_insert_only, produce_tables_once, check_and_create_topic
from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA, DBT_MODELS_JSON_DIR, THRIFT_HOST, THRIFT_PORT, DBT_MODELS_SQL_DIR,
    KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID, STAGING_SCHEMA, RAW_VAULT_SCHEMA, PROCESSING_MODE,
    KAFKA_MAX_OFFSETS_PER_TRIGGER, RAW_VAULT_BASE_PATH, STAGING_BASE_PATH,
    DBT_DEBOUNCE_SECONDS, DBT_MAX_MODELS_PER_RUN, STREAM_TRIGGER, )
from dv_modeller import extract_metadata, split_datavault
from meta_store import write_lineage, write_metadata
from utils.bronze_ingestor import ensure_bronze_table_exists, start_bronze_writer
from utils.helper_service_ready import wait_for_lake, wait_for_kafka, wait_for_kafka_increase
from utils.helper_spark import get_spark_session, ensure_spark_warehouse_dir, get_active_stream_query_by_name
from utils.maintenance.helper_maintenance import maintenance_watchdog
from utils.performance_logger import PerfListener, log_progress_periodically
from utils.schema_helpers import bronze_target_columns, infer_schema_from_cdc_event

import threading
import time
from typing import Iterable, Set

_DBT_LOCK = threading.Lock()  # serialize dbt runs during streaming

# ----------- global runtime for graceful shutdown -------------------
RUN = SimpleNamespace(stop_event=None, query=None, cdc_thread=None)

# Thread-safe set of pending dbt models to run
_DBT_PENDING_MODELS: Set[str] = set()
_DBT_PENDING_LOCK = threading.Lock()
_DBT_WORKER_THREAD: threading.Thread | None = None
_DBT_WORKER_STOP = threading.Event()

def _graceful_shutdown(signum=None, frame=None):
    """Handle SIGTERM/SIGINT and atexit: stop CDC + drain/stop Spark cleanly."""
    try:
        log.info(f"[SHUTDOWN] signal={signum} received, draining ......")
    except Exception:
        pass

    try:
        if RUN.stop_event is not None:
            RUN.stop_event.set()
    except Exception:
        pass

    q = getattr(RUN, "query", None)
    if q is not None:
        try:
            if q.isActive:
                q.stop()  # drain in-flight micro batches
        except Exception as e:
            try:
                log.warning(f"[SHUTDOWN] query.stop() failed: {e}]")
            except Exception:
                pass

    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()
    except Exception:
        pass

    try:
        t = getattr(RUN, "cdc_thread", None)
        if t is not None and t.is_alive():
            t.join(timeout=20)
    except Exception:
        pass


# register handlers early to catch signals during bootstrap
signal.signal(signal.SIGTERM, _graceful_shutdown)
signal.signal(signal.SIGINT, _graceful_shutdown)
atexit.register(_graceful_shutdown)


class _SingletonRunLock():
    """
    Best-effort singelton run guard to avoid two orchestrators booting oncurrently.
    """

    def __init__(self, path="/tmp/dv_orchestrator.lock"):
        self.path = path
        self._fh = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "w")
        fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._fh.write(str(os.getpid()))
        self._fh.flush()
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
        except Exception:
            pass


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

def queue_dbt_models(models: Iterable[str]) -> None:
    """Collect models to run; actual run is done by the debouncer thread."""
    if not models:
        return
    with _DBT_PENDING_LOCK:
        for m in models:
            if m:  # guard against Nones/empties
                _DBT_PENDING_MODELS.add(m)

def _drain_models(max_models: int | None = None) -> list[str]:
    """Atomically take up to max_models models from the pending set."""
    with _DBT_PENDING_LOCK:
        if not _DBT_PENDING_MODELS:
            return []
        if max_models is None or max_models >= len(_DBT_PENDING_MODELS):
            batch = sorted(_DBT_PENDING_MODELS)
            _DBT_PENDING_MODELS.clear()
            return batch
        # take a bounded slice to avoid huge single runs (optional)
        batch = sorted(list(_DBT_PENDING_MODELS)[:max_models])
        _DBT_PENDING_MODELS.difference_update(batch)
        return batch

def _dbt_worker_loop(interval_seconds: int, max_models_per_run: int) -> None:
    """Background loop that periodically runs dbt for accumulated models."""
    log.info("[DBT-DEBOUNCER] started: interval=%ss, max_models_per_run=%s",
             interval_seconds, max_models_per_run)
    try:
        next_wakeup = time.time() + interval_seconds
        while not _DBT_WORKER_STOP.is_set():
            now = time.time()

            # If we've hit the max pending models, run immediately.
            with _DBT_PENDING_LOCK:
                pending_count = len(_DBT_PENDING_MODELS)

            if pending_count >= max_models_per_run:
                batch = _drain_models(max_models_per_run)
                if batch:
                    log.info("[DBT-DEBOUNCER] early run (size=%s): %s", len(batch), batch)
                    run_dbt_models(batch)
                next_wakeup = now + interval_seconds

            # Normal wake-up
            timeout = max(0.0, next_wakeup - now)
            _DBT_WORKER_STOP.wait(timeout)
            if _DBT_WORKER_STOP.is_set():
                break

            # Periodic run
            batch = _drain_models(max_models_per_run)
            if batch:
                log.info("[DBT-DEBOUNCER] periodic run (size=%s): %s", len(batch), batch)
                run_dbt_models(batch)
            next_wakeup = time.time() + interval_seconds

        # Drain anything left on shutdown
        final = _drain_models(None)
        if final:
            log.info("[DBT-DEBOUNCER] draining on shutdown (size=%s): %s", len(final), final)
            run_dbt_models(final)
    finally:
        log.info("[DBT-DEBOUNCER] stopped")

def start_dbt_debouncer() -> None:
    """Start the debouncer worker thread once."""
    global _DBT_WORKER_THREAD
    if _DBT_WORKER_THREAD and _DBT_WORKER_THREAD.is_alive():
        return
    t = threading.Thread(
        target=_dbt_worker_loop,
        args=(DBT_DEBOUNCE_SECONDS, DBT_MAX_MODELS_PER_RUN),
        name="dbt-debouncer",
        daemon=True,
    )
    _DBT_WORKER_STOP.clear()
    t.start()
    _DBT_WORKER_THREAD = t

def stop_dbt_debouncer() -> None:
    """Signal the debouncer to stop and wait briefly."""
    _DBT_WORKER_STOP.set()
    t = _DBT_WORKER_THREAD
    if t and t.is_alive():
        t.join(timeout=15)

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
            return pd.DataFrame(columns=cols)

        return tables, load_table
    else:
        raise ValueError(f"Unsupported LAKE_TYPE: {LAKE_TYPE}")

    return tables, load_table


def ensure_database_schema():
    """Create target Spark database/schema with a LOCATION if configured."""
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

    # Map known schemas to their base paths
    schema_locations = {
        STAGING_SCHEMA: RAW_VAULT_BASE_PATH,
        RAW_VAULT_SCHEMA: RAW_VAULT_BASE_PATH,
    }
    desired_loc = schema_locations.get(schema)

    host, port, user = _resolve_thrift(target_cfg)
    conn = hive.Connection(host=host, port=port, username=user)
    cursor = conn.cursor()

    if desired_loc:
        Path(desired_loc).mkdir(parents=True, exist_ok=True)
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema} LOCATION '{desired_loc}'")
        log.info(f"[DB] Ensured database/schema '{schema}' exists at {desired_loc}")
    else:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {schema}")
        log.info(f"[DB] Ensured database/schema '{schema}' exists")

    if desired_loc:
        cursor.execute(f"DESCRIBE DATABASE EXTENDED {schema}")
        rows = cursor.fetchall()
        current_loc = next((r[1] for r in rows if str(r[0]).lower() == "location"), None)
        if current_loc and current_loc.rstrip("/") != desired_loc.rstrip("/"):
            cursor.execute(f"ALTER DATABASE {schema} SET LOCATION '{desired_loc}'")
            log.info(f"[DB] Moved default LOCATION of {schema} to {desired_loc}")

    cursor.close()
    conn.close()


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

    src_name = meta.get("source_name") or "staging"

    # --- config block: literal unique_key + merge ---------------------------
    unique_key = (business_keys + ["hashdiff"]) if mtype == "sat" else business_keys
    if (mtype in {"hub", "link"}) and not business_keys:
        raise ValueError(f"{mtype} model requires business_keys")

    config_lines = [
        "materialized='incremental'",
        "file_format='delta'",
        "on_schema_change='append_new_columns'",
        "incremental_strategy='merge'",
        f"unique_key={unique_key!r}"
    ]
    incremental_conf = "{{ config(\n  " + ",\n  ".join(config_lines) + "\n) }}\n"

    # helper to keep jinja braces intact
    def jinja_source(src, tbl):
        return "{{ source('" + src + "', '" + tbl + "') }}"

    # --- SELECT -------------------------------------------------------------
    lines = [incremental_conf, "select"]

    if mtype in {"hub", "link"}:
        # keys + audit
        for k in business_keys:
            lines.append("    " + k + ",")
        lines.append("    current_timestamp() as load_datetime,")
        lines.append("    '" + table_name + "' as record_source")
        lines.append("from " + jinja_source(src_name, table_name))
        lines.append("group by " + ", ".join(business_keys))

    else:  # sat
        # keys + attributes + hashdiff + audit
        for c in business_keys + attributes:
            lines.append("    " + c + ",")
        if attributes:
            attrs_expr = ", ".join("coalesce(cast(" + c + " as string), '')" for c in attributes)
            lines.append("    sha2(concat_ws('||', " + attrs_expr + "), 256) as hashdiff,")
        else:
            lines.append("    sha2('', 256) as hashdiff,")
        lines.append("    current_timestamp() as load_datetime,")
        lines.append("    '" + table_name + "' as record_source")
        lines.append("from " + jinja_source(src_name, table_name))

    content = "\n".join(lines) + "\n"
    wrote = write_text_if_changed(file_path, content)
    if wrote:
        # keep this an f-string so we can see the real file name
        log.debug(f"[DBT] Wrote SQL model {model_name}.sql")
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

    model_def = {
        "model_name": model_name,
        "table_name": table_name,
        "model_type": mtype,
        "business_keys": list(meta.get("business_keys", [])),
        "attributes": list(meta.get("attributes", [])),
        "columns": list(meta.get("columns", [])),
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
    # TODO: fix the creation of the file if not exists, to create the "real" one, if not shipped
    if not os.path.exists(DBT_PROFILES_DIR):
        os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
        log.info(f"[INFO] Created dbt profiles directory: {DBT_PROFILES_DIR}")

    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        with open(profiles_yml_path, "w") as f:
            f.write("# Insert your dbt profile config here\n")
        log.info(f"[INFO] Created empty profiles.yml at: {profiles_yml_path}")


def ensure_real_profiles():
    """
    Force DBT_PROFILES_DIR to the repo's profiles/ folder.
    Will override any attempt to create empty profiles.
    """
    # Repo root (assuming app runs in /app)
    repo_profiles = Path(__file__).resolve().parent.parent / "profiles"
    if not repo_profiles.exists():
        raise RuntimeError(f"profiles/ directory not found at {repo_profiles}")
    os.environ["DBT_PROFILES_DIR"] = str(repo_profiles)
    return repo_profiles


# Spark consumer (Streaming + model generation)
def get_kafka_stream(spark, table_name, schema):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("cdc_modified_at", StringType())
    ])
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)  # earlist for first run, then checkpoint
        .option("groupIdPrefix", KAFKA_GROUP_ID)
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    df_data = df_table.select(from_json(col("json.payload"), schema).alias("data")).select("data.*")
    return df_data


def streaming_dv_consumer_and_dbt(models_to_run):
    """
    Generic, schema-late binding Kafka -> Bronze streaming consumer + dbt trigger.
    Writes bronze tables as Delta, auto-creating them, and triggers dbt per touched table.
    """
    import os, json, shutil, threading  # FIX: add threading
    from pyspark.sql import functions as F  # FIX: F used later
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType

    spark = get_spark_session("DataVault_Streaming_Consumer")
    try:
        spark.conf.set("spark.sql.sources.default", "delta")  # FIX: safer default
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception:
        pass

    try:
        spark.streams.addListener(PerfListener())
    except Exception as e:
        log.debug("PerfListener attach skipped: %s", e)

    try:
        spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    except Exception:
        pass

    envelope_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("cdc_modified_at", StringType()),
    ])

    checkpoint_root = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")
    checkpoint_dir = os.path.join(checkpoint_root, f"{KAFKA_TOPIC}_generic_v3")

    ## TODO: ONLY FOR TESTING, REMOVE BEFORE DEPLOYMENT
    if os.environ.get("STREAM_CHECKPOINT_RESET", "").lower() in {"1", "true", "yes"}:
        log.warning("[STREAM] Wiping checkpoint dir: %s", checkpoint_dir)
        shutil.rmtree(checkpoint_dir, ignore_errors=True)

    log.info("[STREAM][source] bootstrap=%s topic=%s startingOffsets=earliest", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    src = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.metadata.max.age.ms", "2000")
        .option("kafka.partition.discovery.interval.ms", "2000")
        .option("kafkaConsumer.pollTimeoutMs", "1000")
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
        .select(from_json(col("value").cast("string"), envelope_schema).alias("json"))
        .where(col("json").isNotNull())
        .select(
            col("json.table").alias("table"),
            col("json.payload").alias("payload"),
            col("json.cdc_type").alias("cdc_type"),
            col("json.cdc_modified_at").alias("cdc_modified_at"),
        )
        .where(col("table").isNotNull())
    )

    table_to_models = get_existing_model_tables()

    def _infer_schema_from_batch(df_tbl):
        rows = df_tbl.select("payload").limit(1).collect()
        if not rows:
            return None
        try:
            obj = json.loads(rows[0]["payload"])
            return StructType([StructField(k, StringType(), True) for k in obj.keys()])
        except Exception as e:
            log.debug("Inline schema inference failed: %s", e)
            return None

    def _process_table(batch_df, tbl, epoch_id):
        """
        - infers schema for `tbl`
        - writes Bronze (Delta) for this table
        - DOES NOT call dbt here (dbt is triggered after batch via debouncer)
        """
        schema = _infer_schema_from_batch(batch_df) or infer_schema_from_cdc_event(spark, tbl)
        if not schema:
            log.info("[STREAM][%s][epoch=%s] no schema available; skipping", tbl, epoch_id)
            return None

        parsed_tbl = batch_df.select(from_json(col("payload"), schema).alias("r")).select("r.*").coalesce(8)

        batch_count = parsed_tbl.count()
        if batch_count == 0:
            log.debug("[STREAM][%s][epoch=%s] empty micro-batch; skipping", tbl, epoch_id)
            return None

        # Write Bronze (keep your existing write path/options)
        table_path = os.path.join(STAGING_BASE_PATH, tbl)
        (parsed_tbl.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(table_path)
         )
        log.info("[BRONZE][%s][epoch=%s] wrote %s rows to %s", tbl, epoch_id, batch_count, table_path)
        return tbl

    def _foreach_batch(batch_df, epoch_id: int):
        if batch_df.rdd.isEmpty():
            log.debug("[STREAM][epoch=%s] empty micro-batch", epoch_id)
            return
        touched = [r["table"] for r in batch_df.select("table").distinct().collect() if r["table"]]
        if not touched:
            log.debug("[STREAM][epoch=%s] no 'table' values present", epoch_id)
            return
        for tbl in touched:
            try:
                _process_table(batch_df.where(col("table") == tbl), tbl, epoch_id)
            except Exception as e:
                log.exception("[STREAM][%s][epoch=%s] processing failed: %s", tbl, epoch_id, e)


    table_to_models = get_existing_model_tables()

    # Use your unified writer (now fixed to force Delta)
    # Be tolerant to different callback signatures from bronze_ingestor
    def _after_write(*args, **kwargs):
        # Accept (touched,) or (touched, batch_id, counts)
        if not args:
            return
        written_tables = args[0] or []
        if not written_tables:
            return

        models = set()
        for tbl in written_tables:
            for m in table_to_models.get(tbl, []):
                models.add(m)
        if models:
            log.info("[DBT-QUEUE] epoch models=%s", sorted(models))
            queue_dbt_models(models)

    allowed_tables = set(table_to_models.keys()) if table_to_models else None

    query = start_bronze_writer(
        spark=spark,
        df_stream=src,
        table_name=None,
        table_col="table",
        checkpoint_base=checkpoint_dir,
        on_after_write=_after_write,
        allowed_tables=allowed_tables,
        query_name=f"{KAFKA_TOPIC}-generic-ingestor",
        trigger_every=STREAM_TRIGGER,
    )

    log.info("[STREAM] Query started: id=%s, name=%s, trigger=%s, checkpoint=%s",
             query.id, query.name, STREAM_TRIGGER, checkpoint_dir)
    # threading.Thread(target=log_progress_periodically, args=(query,), daemon=True).start()
    return query


def bootstrap_bronze(lake_tables, load_table):
    from pyspark.sql.types import StructType, StructField, StringType
    spark = get_spark_session("DataVault_Bootstrap")
    spark.streams.addListener(PerfListener())

    for t in lake_tables:
        # Get column names from the lake; make a simple all-STRING schema for the empty Bronze
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

    # wait for lake readiness before discovery
    wait_for_lake(timeout_sec=60)

    with _SingletonRunLock():
        # -------- Phase 0: Discover lake + bootstrap Bronze/DBT scaffolding --------
        lake_tables, load_table = discover_lake()
        bootstrap_bronze(lake_tables, load_table)  # precreate empty bronze tables (DDL)

        generate_schema_yml(lake_tables)
        existing_models = get_existing_model_tables()
        vault_tables = get_raw_vault_tables()

        # Determine which tables are new (no model yet) and which vault objects are missing
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
            # Create raw_vault objects up-front (first run); later runs will also be triggered by streaming callback
            run_dbt_models(sorted(models_to_run))
        # Start the debounced DBT runner (coalesces per-batch model requests)
        start_dbt_debouncer()

        # -------- Phase 1: Kafka readiness + topic ensure --------
        check_and_create_topic()  # make sure topic exists before streams/producers
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, timeout_sec=60)

        # Mode switch for CRON bulk runs
        processing_mode = PROCESSING_MODE.lower()
        if processing_mode not in {"streaming", "bulk"}:
            log.warning(f"[MODE] Unknown PROCESSING={PROCESSING_MODE} -> defaulting to 'streaming'")
            processing_mode = "streaming"

        # -------- Phase 2: Stream up (consumer) --------
        stop_event = threading.Event()
        RUN.stop_event = stop_event
        query = None
        query_holder = {"q": None}
        if processing_mode == "streaming":
            # Start stream and keep handle (non-blocking; returns StreamingQuery)
            query = streaming_dv_consumer_and_dbt(models_to_run)
            query_holder["q"] = query
            RUN.query = query
            # Small settle time so Spark attaches before initial production
            try:
                time.sleep(1)
            except Exception:
                pass
            # Maintenance watchdog (pause/resume around daily prune)
            threading.Thread(
                target=maintenance_watchdog,
                args=(query, streaming_dv_consumer_and_dbt, (models_to_run,)),
                daemon=True,
                name="Maintenance Watchdog",
            ).start()
        # -------- Phase 3: Initial full load (backlog) --------
        if processing_mode == 'bulk':
            # TODO: IMPLEMENT CRON JOB FRIENDLY PROCESSING ---> SEE DataLake service
            log.info("[PROCESSING-MODE] BULK: producing once for all lake tables and exiting")
            produced_map = produce_tables_once(sorted(lake_tables))  # CRON-friendly one shot
            # Assert that Kafka actually received what we produced
            wait_for_kafka_increase(sum(produced_map.values()), timeout_sec=60)
            return
        else:
            # 1) Take Kafka baseline BEFORE producing anything
            from utils.helper_service_ready import kafka_total_end, wait_for_kafka_total_at_least
            base_total = kafka_total_end(
                bootstrap=KAFKA_BOOTSTRAP_SERVERS,
                topic=os.getenv("KAFKA_TOPIC", "lake_stream"),
            )
            log.info("[ASSERT][KAFKA_OFFSETS][BASE] bootstrap=%s topic=%s base_total=%s",
                     KAFKA_BOOTSTRAP_SERVERS, os.getenv("KAFKA_TOPIC", "lake_stream"), base_total)

            produced_once = set()
            produced_total = 0

            if tables_needing_initial_load:
                todo = sorted(list(tables_needing_initial_load))
                log.info("[INITIAL LOAD] Producing full load for tables: %s", todo)
                produced_map = produce_tables_once(todo) or {}  # make sure this returns a dict
                produced_total += sum(produced_map.values())
                produced_once |= set(todo)

            remaining = [t for t in lake_tables if t not in produced_once]
            if remaining:
                log.info("[INITIAL LOAD] Producing full load for remaining tables: %s", remaining)
                produced_map = produce_tables_once(remaining) or {}
                produced_total += sum(produced_map.values())

            # 2) Gate on Kafka reaching base + produced_total
            target_total = base_total + produced_total
            wait_for_kafka_total_at_least(
                min_total=target_total,
                timeout_sec=60,
                bootstrap=KAFKA_BOOTSTRAP_SERVERS,
                topic=os.getenv("KAFKA_TOPIC", "lake_stream"),
            )

            # 3) gate on Spark seeing the growth
            from utils.helper_service_ready import wait_for_stream_offset_growth
            q = get_active_stream_query_by_name("lake_stream-generic-ingestor") or query  # small helper you add
            if q:
                wait_for_stream_offset_growth(q, produced_total=produced_total, base_total=base_total, timeout_sec=60)
                log.info("[STREAM][status] isActive=%s", q.isActive)
                lp = q.lastProgress or {}
                log.info("[STREAM][source-desc] %s", (lp.get("sources", [{}])[0].get("description")))

        # -------- Phase 4: Continuous CDC producer (insert-only) --------
        def _cdc_loop():
            try:
                cdc_producer_insert_only(stop_event=stop_event)
            except TypeError:
                cdc_producer_insert_only()

        cdc_thread = threading.Thread(target=_cdc_loop, daemon=False, name="cdc-insert-only")
        RUN.cdc_thread = cdc_thread
        cdc_thread.start()

        # -------- Phase 5: Lifecycle / graceful shutdown --------
        try:
            if query is not None:
                # Block until SIGTERM/SIGINT or query.stop()
                query.awaitTermination()
            else:
                # Non-streaming mode: idle but responsive to signals
                while not stop_event.is_set():
                    time.sleep(1)
        except KeyboardInterrupt:
            _graceful_shutdown()
        finally:
            try:
                stop_dbt_debouncer()
            finally:
                _graceful_shutdown()


if __name__ == "__main__":
    main()
