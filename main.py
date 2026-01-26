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
from psycopg2.pool import SimpleConnectionPool
from pyhive import hive

from cdc_kafka_producer import cdc_producer_insert_only, produce_tables_once, check_and_create_topic
from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA, DBT_MODELS_JSON_DIR, THRIFT_HOST, THRIFT_PORT, DBT_MODELS_SQL_DIR,
    KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID, STAGING_SCHEMA, RAW_VAULT_SCHEMA, PROCESSING_MODE,
    KAFKA_MAX_OFFSETS_PER_TRIGGER, RAW_VAULT_BASE_PATH, STAGING_BASE_PATH,
    DBT_DEBOUNCE_SECONDS, DBT_MAX_MODELS_PER_RUN, STREAM_TRIGGER, POSTGRES_POOL_MAX, POSTGRES_POOL_MIN)
from dv_modeller import extract_metadata, split_datavault
from meta_store import write_lineage, write_metadata
from utils.bronze_ingestor import ensure_bronze_table_exists, start_bronze_writer, ensure_bronze_table_schema
from utils.helper_service_ready import wait_for_lake, wait_for_kafka, wait_for_kafka_increase
from utils.helper_spark import get_spark_session, ensure_spark_warehouse_dir, get_active_stream_query_by_name
from utils.maintenance.helper_maintenance import maintenance_watchdog
from utils.performance_logger import PerfListener, log_progress_periodically
from utils.schema_helpers import bronze_target_columns, infer_schema_from_cdc_event

import threading
import time
from typing import Iterable, Set, Optional

PG_POOL = None
_PG_POOL_LOCK = threading.Lock()

_SPARK = None
_SPARK_READY = threading.Event()
# ----------- global runtime for graceful shutdown -------------------
RUN = SimpleNamespace(stop_event=None, query=None, cdc_thread=None)

_DBT_LOCK = threading.Lock()                 # serializes actual dbt subprocess runs
_DBT_PENDING_LOCK = threading.Lock()         # protects pending set + persistence
_DBT_WORKER_STOP = threading.Event()         # stop signal for worker
_DBT_WAKE = threading.Event()                # wake signal when new models arrive
_DBT_WORKER_THREAD: Optional[threading.Thread] = None

_DBT_PENDING_MODELS: set[str] = set()
_DBT_PENDING_LOADED = False

# Persist pending models so a container restart (fault injection) does not lose queued work.
_DBT_PENDING_FILE = os.getenv("DBT_PENDING_MODELS_FILE", "/data/state/dbt_pending_models.json")


_DBT_PREFLIGHT_SPARK = None
_DBT_PREFLIGHT_LOCK = threading.Lock()

def _get_dbt_preflight_spark():
    global _DBT_PREFLIGHT_SPARK
    if _DBT_PREFLIGHT_SPARK is None:
        with _DBT_PREFLIGHT_LOCK:
            if _DBT_PREFLIGHT_SPARK is None:
                _DBT_PREFLIGHT_SPARK = get_spark_session("DataVault_DBT_Preflight")
    return _DBT_PREFLIGHT_SPARK

def _load_pending_models_from_disk() -> None:
    """Load pending models once per process start (idempotent)."""
    global _DBT_PENDING_LOADED, _DBT_PENDING_MODELS
    if _DBT_PENDING_LOADED:
        return
    _DBT_PENDING_LOADED = True

    try:
        if not os.path.exists(_DBT_PENDING_FILE):
            return
        with open(_DBT_PENDING_FILE, "r", encoding="utf-8") as f:
            data = json.load(f) or []
        if isinstance(data, list):
            cleaned = {str(x).strip() for x in data if str(x).strip()}
            _DBT_PENDING_MODELS |= cleaned
    except Exception as exc:
        log.warning(f"[DBT-DEBOUNCER] Failed to load pending models from {_DBT_PENDING_FILE}: {exc}")


def _persist_pending_models_to_disk() -> None:
    """Atomically persist pending models."""
    try:
        os.makedirs(os.path.dirname(_DBT_PENDING_FILE), exist_ok=True)
        tmp = _DBT_PENDING_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_DBT_PENDING_MODELS), f)
        os.replace(tmp, _DBT_PENDING_FILE)
    except Exception as exc:
        log.warning(f"[DBT-DEBOUNCER] Failed to persist pending models to {_DBT_PENDING_FILE}: {exc}")


def queue_dbt_models(models: Iterable[str]) -> None:
    """
    Collect models to run; actual run is done by the debouncer thread.
    Key properties:
      - Safe to call before the debouncer is started
      - Wakes the debouncer immediately
      - Persists pending set to survive restarts
    """
    models = [m for m in (models or []) if m and str(m).strip()]
    if not models:
        return

    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()
        before = len(_DBT_PENDING_MODELS)
        _DBT_PENDING_MODELS.update(str(m).strip() for m in models)
        if len(_DBT_PENDING_MODELS) != before:
            _persist_pending_models_to_disk()

    # Wake worker so it doesn't wait the full debounce interval.
    _DBT_WAKE.set()


def _drain_models(max_models: Optional[int] = None) -> list[str]:
    """Atomically take up to max_models models from the pending set (and persist)."""
    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()
        if not _DBT_PENDING_MODELS:
            return []

        if max_models is None or max_models >= len(_DBT_PENDING_MODELS):
            batch = sorted(_DBT_PENDING_MODELS)
            _DBT_PENDING_MODELS.clear()
            _persist_pending_models_to_disk()
            return batch

        batch = sorted(list(_DBT_PENDING_MODELS)[: max_models])
        _DBT_PENDING_MODELS.difference_update(batch)
        _persist_pending_models_to_disk()
        return batch


def _requeue(batch: list[str]) -> None:
    """Re-queue a batch after a failed dbt run."""
    if not batch:
        return
    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()
        _DBT_PENDING_MODELS.update(batch)
        _persist_pending_models_to_disk()
    _DBT_WAKE.set()


def _dbt_worker_loop(interval_seconds: int, max_models_per_run: int) -> None:
    """
    Background loop that runs dbt for accumulated models.
    - Wake-on-queue for fast reaction
    - Retry by re-queuing on failure
    """
    log.info(
        "[DBT-DEBOUNCER] started: interval=%ss, max_models_per_run=%s",
        interval_seconds, max_models_per_run
    )

    try:
        while not _DBT_WORKER_STOP.is_set():
            # Wait for either a wake signal or the periodic interval.
            _DBT_WAKE.wait(timeout=float(interval_seconds))
            _DBT_WAKE.clear()

            if _DBT_WORKER_STOP.is_set():
                break

            batch = _drain_models(max_models_per_run)
            if not batch:
                continue

            try:
                log.info("[DBT-DEBOUNCER] running (size=%s): %s", len(batch), batch)
                # run_dbt_models must exist in main.py already
                run_dbt_models(batch)
            except Exception as exc:
                # Important: do not drop the batch; requeue for retry.
                log.warning(f"[DBT-DEBOUNCER] dbt run failed; re-queueing batch. error={exc}")
                _requeue(batch)
                # backoff a bit to avoid tight loops on persistent failures
                time.sleep(min(10.0, float(interval_seconds)))

        # Drain on shutdown
        final = _drain_models(None)
        if final:
            try:
                log.info("[DBT-DEBOUNCER] draining on shutdown (size=%s): %s", len(final), final)
                run_dbt_models(final)
            except Exception as exc:
                log.warning(f"[DBT-DEBOUNCER] final drain failed (dropping). error={exc}")

    finally:
        log.info("[DBT-DEBOUNCER] stopped")


def start_dbt_debouncer() -> None:
    """Start the debouncer worker thread once. Safe to call from gRPC thread."""
    global _DBT_WORKER_THREAD
    if _DBT_WORKER_THREAD and _DBT_WORKER_THREAD.is_alive():
        return

    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()

    _DBT_WORKER_STOP.clear()
    t = threading.Thread(
        target=_dbt_worker_loop,
        args=(int(DBT_DEBOUNCE_SECONDS), int(DBT_MAX_MODELS_PER_RUN)),
        name="dbt-debouncer",
        daemon=True,
    )
    t.start()
    _DBT_WORKER_THREAD = t


def stop_dbt_debouncer() -> None:
    """Signal the debouncer to stop and wake it so it exits promptly."""
    _DBT_WORKER_STOP.set()
    _DBT_WAKE.set()
    t = _DBT_WORKER_THREAD
    if t and t.is_alive():
        t.join(timeout=15)


# gRPC server threading primitives
VAULT_GRPC_STOP_EVENT = threading.Event()
VAULT_GRPC_THREAD: threading.Thread | None = None

def _is_pool_closed_error(exc: Exception) -> bool:
    return "pool is closed" in str(exc).lower()


def _is_pool_exhausted_error(exc: Exception) -> bool:
    # psycopg2.pool raises PoolError("connection pool exhausted")
    return "connection pool exhausted" in str(exc).lower()


def _reset_postgres_pool(reason: str = "") -> None:
    """Close and discard the global pool (safe to call multiple times)."""
    global PG_POOL
    with _PG_POOL_LOCK:
        pool = PG_POOL
        PG_POOL = None  # IMPORTANT: ensure next init truly recreates
    if pool is not None:
        try:
            pool.closeall()
        except Exception:
            pass
    if reason:
        log.warning("PostgreSQL pool reset (%s).", reason)

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

    try:
        VAULT_GRPC_STOP_EVENT.set()
    except Exception:
        pass

    try:
        if VAULT_GRPC_THREAD is not None and VAULT_GRPC_THREAD.is_alive():
            VAULT_GRPC_THREAD.join(timeout=5)
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

def _wait_bronze_stable_rows(
    spark,
    schema: str,
    table: str,
    *,
    min_rows: int = 1,
    stable_checks: int = 2,
    interval_s: float = 2.0,
    timeout_s: float = 120.0,
) -> int:
    """
    Wait until SELECT COUNT(*) from schema.table is:
      - >= min_rows
      - stable across `stable_checks` consecutive polls

    This prevents dbt raw-vault runs from snapshotting bronze while ingestion is still in progress.
    """
    import time

    deadline = time.time() + float(timeout_s)
    last = None
    stable = 0

    while time.time() < deadline:
        try:
            cnt = spark.sql(f"SELECT COUNT(*) AS c FROM {schema}.{table}").collect()[0]["c"]
            cnt = int(cnt)
        except Exception:
            cnt = 0

        if cnt >= min_rows:
            if last is not None and cnt == last:
                stable += 1
            else:
                stable = 0
            last = cnt

            if stable >= (stable_checks - 1):
                return cnt

        time.sleep(float(interval_s))

    return int(last or 0)

def _preflight_bronze_for_tables(tables: Iterable[str]) -> None:
    """
    Ensure bronze.<table> exists and has at least the columns expected from the lake schema.

    This is required for gRPC-triggered dbt runs where schema evolution may occur before
    any CDC payload contains the new column (e.g., email).
    """
    if not tables:
        return

    try:
        spark = _get_dbt_preflight_spark()
    except Exception as e:
        log.warning(f"[DBT] Spark not available for bronze preflight: {e}")
        return

    from pyspark.sql.types import StructType, StructField, StringType
    import time

    timeout_s = float(os.getenv("DBT_BRONZE_PREFLIGHT_TIMEOUT_S", "120"))
    poll_s = float(os.getenv("DBT_BRONZE_PREFLIGHT_POLL_S", "2"))

    for t in tables:
        tbl = str(t or "").strip().lower()
        if not tbl:
            continue

        # Wait until the lake schema is discoverable (delta table created on first write)
        deadline = time.time() + timeout_s
        cols = []
        logged = False

        while time.time() < deadline:
            cols = bronze_target_columns(spark, tbl) or []
            if cols:
                break

            if not logged:
                log.info(
                    f"[DBT] Bronze preflight waiting for lake schema: table={tbl} "
                    f"(timeout_s={timeout_s}, poll_s={poll_s})"
                )
                logged = True

            time.sleep(poll_s)

        if not cols:
            # Do NOT continue into dbt; that will fail with a cryptic TABLE_OR_VIEW_NOT_FOUND.
            raise RuntimeError(
                f"[DBT] Bronze preflight timed out waiting for lake schema for '{tbl}' "
                f"(timeout_s={timeout_s}). Refusing to run dbt because bronze.{tbl} would be missing."
            )

        schema = StructType([StructField(str(c), StringType(), True) for c in cols])

        # Ensure table is registered as external Delta and enforce missing cols.
        ensure_bronze_table_exists(spark, tbl, schema)
        ensure_bronze_table_schema(spark, tbl, schema)

        # Wait until bronze row count stabilizes (prevents empty/partial raw_vault tables).
        try:
            stable_cnt = _wait_bronze_stable_rows(
                spark,
                STAGING_SCHEMA if "STAGING_SCHEMA" in globals() else "bronze",
                tbl,
                min_rows=1,
                stable_checks=3,
                interval_s=2.0,
                timeout_s=180.0,
            )
            log.info(f"[DBT] Bronze preflight ready: {tbl} stable_rows={stable_cnt}")
        except Exception as e:
            log.warning(f"[DBT] Bronze row-count stability check failed for {tbl}: {e}")

def run_dbt_models(models):
    """Run dbt for the specified models."""
    if not models:
        return

    ensure_profiles_dir()

    base_dir = os.path.dirname(globals().get("__file__", os.getcwd()))
    schema_path = os.path.join(base_dir, "models", "schema.yml")

    referenced_tables = set()
    for m in models:
        meta_path = os.path.join(DBT_MODELS_JSON_DIR, f"{m}.json")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                j = json.load(f)
            t = (j.get("table_name") or "").strip()
            if t:
                referenced_tables.add(t)
        except FileNotFoundError:
            continue
        except Exception as e:
            log.warning(f"[DBT] Could not read model metadata for {m}: {e}")

    # Normalize table ids to match how bronze is actually named/registered
    referenced_tables = {str(t).strip().lower() for t in referenced_tables if str(t).strip()}

    # NEW: Ensure bronze sources exist + have the expected columns before dbt reads them
    _preflight_bronze_for_tables(sorted(referenced_tables))

    # Merge with existing schema.yml tables (avoid thrash)
    existing_tables = set()
    try:
        if os.path.exists(schema_path):
            with open(schema_path, "r", encoding="utf-8") as f:
                doc = yaml.safe_load(f) or {}
            for src in (doc.get("sources") or []):
                if (src.get("name") == "staging") and isinstance(src.get("tables"), list):
                    for t in src["tables"]:
                        if isinstance(t, dict) and t.get("name"):
                            existing_tables.add(str(t["name"]).strip().lower())
    except Exception as e:
        log.warning(f"[DBT] Could not parse existing schema.yml (will regenerate): {e}")

    all_model_tables = set()
    try:
        if os.path.exists(DBT_MODELS_JSON_DIR):
            for fname in os.listdir(DBT_MODELS_JSON_DIR):
                if not fname.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(DBT_MODELS_JSON_DIR, fname), "r", encoding="utf-8") as f:
                        j = json.load(f)
                    t = (j.get("table_name") or "").strip()
                    if t:
                        all_model_tables.add(str(t).strip().lower())
                except Exception:
                    # Best-effort: ignore malformed/partial files
                    continue
    except Exception as e:
        log.warning(f"[DBT] Could not scan model metadata directory for schema sources: {e}")

    merged = sorted(existing_tables.union(referenced_tables).union(all_model_tables))
    generate_schema_yml(merged, output_path=schema_path)

    cmd = [
        "dbt",
        "run",
        "--profiles-dir",
        DBT_PROFILES_DIR,
        "--select",
    ] + sorted(models)

    log.info(f"[DBT] Running: {cmd}")

    with _DBT_LOCK:
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.stdout:
            log.info(proc.stdout)
        if proc.stderr:
            log.error(proc.stderr)

        if proc.returncode != 0:
            raise RuntimeError(f"dbt failed (rc={proc.returncode})")

def _resolve_thrift(target_cfg):
    """
    Resolve Hive Thrift connection parameters with env taking precedence.

    Drop-in hardening:
      - Supports dbt-style jinja in profiles.yml (e.g. {{ env_var('USER', 'dbt') }})
      - Allows explicit THRIFT_USER override
      - Avoids usernames that do not exist in the container (defaults to 'root' for tests)
    """
    env_host = os.environ.get("THRIFT_HOST")
    env_port = os.environ.get("THRIFT_PORT")
    env_user = os.environ.get("THRIFT_USER")

    host = env_host or target_cfg.get("host") or THRIFT_HOST
    port_raw = env_port or target_cfg.get("port") or THRIFT_PORT
    user = env_user or target_cfg.get("user") or os.environ.get("USER") or "root"

    # Render jinja templates if present (dbt profiles often contain {{ env_var(...) }}).
    # We only render the env_var(...) pattern, which is what your config uses.
    if isinstance(host, str) and "{{" in host:
        host = Template(host).render(env_var=lambda name, default=None: os.getenv(name, default))

    if isinstance(port_raw, str) and "{{" in port_raw:
        port_raw = Template(port_raw).render(env_var=lambda name, default=None: os.getenv(name, default))

    if isinstance(user, str) and "{{" in user:
        user = Template(user).render(env_var=lambda name, default=None: os.getenv(name, default))

    # Normalize
    host = str(host).strip()
    user = str(user).strip()
    port = int(str(port_raw).strip())

    # Spark/Hadoop group mapping will error if the user does not exist in the container.
    # For docker-compose based tests, safest is root unless explicitly overridden.
    if not user or user == "dbt" or user == "{{ env_var('USER', 'dbt') }}":
        user = "root"

    log.debug(f"Using Hive Thrift server host={host}, port={port}, user={user}")
    return host, port, user


def init_postgres_pool(minconn=None, maxconn=None):
    """
    Initialize and return a global psycopg2 connection pool.
    Recreates the pool if the existing one is unusable/closed.
    """
    global PG_POOL

    if minconn is None:
        minconn = POSTGRES_POOL_MIN
    if maxconn is None:
        maxconn = POSTGRES_POOL_MAX

    with _PG_POOL_LOCK:
        if PG_POOL is not None:
            # Validate the pool is still usable (it can become "closed" after closeall()).
            try:
                c = PG_POOL.getconn()
                PG_POOL.putconn(c)
                log.debug("Reusing existing PostgreSQL connection pool.")
                return PG_POOL
            except Exception as e:
                # Pool became unusable; recreate.
                log.warning("Existing PostgreSQL pool unusable (%s). Recreating.", e)
                old = PG_POOL
                PG_POOL = None
                try:
                    old.closeall()
                except Exception:
                    pass

        # Create a new pool
        try:
            PG_POOL = SimpleConnectionPool(
                minconn,
                maxconn,
                host=RDBMS_HOST,
                port=RDBMS_PORT,
                dbname=RDBMS_DB,
                user=RDBMS_USER,
                password=RDBMS_PASSWORD,
            )
            log.info(f"PostgreSQL connection pool created (min={minconn}, max={maxconn}).")
            return PG_POOL
        except Exception as e:
            log.error(f"Error establishing PostgreSQL connection pool: {e}")
            raise


def connect_postgres():
    """
    Get a connection from the pool.
    Recovers from:
      - pool exhaustion (expand pool)
      - pool closed (recreate pool)
    """
    # Small bounded retry to avoid transient races under load.
    for attempt in range(1, 4):
        pool = init_postgres_pool()
        try:
            conn = pool.getconn()
            if conn.closed:
                log.warning("Received closed connection from pool; replacing it.")
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    pass
                conn = pool.getconn()

            log.debug("Acquired connection from pool.")
            return conn

        except Exception as e:
            # Handle pool exhaustion by expanding pool size.
            if _is_pool_exhausted_error(e):
                try:
                    cur_max = getattr(pool, "maxconn", POSTGRES_POOL_MAX)
                except Exception:
                    cur_max = POSTGRES_POOL_MAX
                new_max = int(cur_max) + 5
                log.warning(f"Connection pool exhausted. Expanding pool to {new_max} connections.")

                # IMPORTANT: ensure a new pool is actually created.
                _reset_postgres_pool("expand")
                init_postgres_pool(POSTGRES_POOL_MIN, new_max)

                # Retry immediately
                continue

            # Handle closed pool by recreating and retrying.
            if _is_pool_closed_error(e):
                _reset_postgres_pool("closed")
                continue

            log.error(f"Error getting connection from pool: {e}")
            raise

        finally:
            # Very small backoff on retries to avoid thundering herd
            if attempt < 3:
                time.sleep(0.05 * attempt)

    # If we got here, we failed repeatedly.
    raise RuntimeError("Failed to acquire Postgres connection from pool after retries.")


def release_postgres_connection(conn):
    """
    Return the connection back to the pool.
    If the pool is gone/closed, close the connection instead of raising.
    """
    global PG_POOL
    if conn is None:
        return

    try:
        pool = init_postgres_pool()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return

    try:
        if conn.closed:
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
            log.debug("Closed dead connection from pool.")
        else:
            pool.putconn(conn)
            log.debug("Released connection back to pool.")
    except Exception as e:
        # If the pool was reset while the connection was in-flight, do not explode.
        log.error(f"Error releasing connection: {e}")
        try:
            conn.close()
        except Exception:
            pass
        if _is_pool_closed_error(e):
            _reset_postgres_pool("release_failed_closed")
def discover_lake():
    """
    Discover lake tables and return:
      - tables: List[str]
      - load_table: Callable[[table_name], pandas.DataFrame(columns=[...])]

    RDBMS mode: introspects Postgres information_schema.
    PARQUET mode: treats PARQUET_PATH as a *Delta Lake root* (e.g. /lake) and discovers
                  per-table directories containing _delta_log/.
    """
    import json
    import os
    from pathlib import Path

    import pandas as pd
    import pyarrow.parquet as pq

    if LAKE_TYPE == "rdbms":
        # ---- existing behaviour (unchanged) ----
        from psycopg2 import sql

        conn = connect_postgres()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (RDBMS_SCHEMA,),
        )
        tables = [r[0] for r in cur.fetchall()]
        cur.close()
        release_postgres_connection(conn)

        def load_table(t: str) -> pd.DataFrame:
            conn2 = connect_postgres()
            cur2 = conn2.cursor()
            q = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(
                sql.Identifier(RDBMS_SCHEMA), sql.Identifier(t)
            )
            cur2.execute(q)
            cols = [desc[0] for desc in cur2.description]
            cur2.close()
            conn2.close()
            return pd.DataFrame(columns=cols)

        return tables, load_table

    # ------------------------------
    # PARQUET MODE (Delta root)
    # ------------------------------
    lake_root = Path(PARQUET_PATH)

    if not lake_root.exists():
        log.warning(f"[discover_lake] PARQUET_PATH does not exist: {lake_root}")
        return [], (lambda _: pd.DataFrame())

    def _is_delta_table_dir(p: Path) -> bool:
        return p.is_dir() and (p / "_delta_log").is_dir()

    def _latest_delta_log_json(delta_log_dir: Path) -> Path | None:
        """
        Delta log consists of JSON commit files like 00000000000000000010.json.
        We pick the highest-numbered JSON file available.
        """
        json_files = sorted(delta_log_dir.glob("*.json"))
        return json_files[-1] if json_files else None

    def _schema_from_delta_log(table_dir: Path) -> list[str] | None:
        """
        Parse schemaString from the delta commit JSON.
        The commit file is newline-delimited JSON objects.
        We find the first object containing 'metaData' with 'schemaString'.
        """
        delta_log_dir = table_dir / "_delta_log"
        commit = _latest_delta_log_json(delta_log_dir)
        if not commit:
            return None

        try:
            with commit.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    obj = json.loads(line)
                    md = obj.get("metaData")
                    if md and "schemaString" in md:
                        schema_str = md["schemaString"]
                        schema_obj = json.loads(schema_str)
                        fields = schema_obj.get("fields", [])
                        return [fld.get("name") for fld in fields if fld.get("name")]
        except Exception as e:
            log.warning(f"[discover_lake] Failed to parse delta log schema for {table_dir}: {e}")

        return None

    def _fallback_schema_from_parquet(table_dir: Path) -> list[str] | None:
        """
        If delta log parsing fails, find any parquet file under the table directory
        and read its schema (fast metadata only).
        """
        try:
            for pf in table_dir.rglob("*.parquet"):
                pf = pf.resolve()
                meta = pq.read_metadata(str(pf))
                return meta.schema.names
        except Exception as e:
            log.warning(f"[discover_lake] Fallback parquet schema failed for {table_dir}: {e}")
        return None

    # Discover delta tables in root (case-insensitive keys).
    table_dirs = [p for p in lake_root.iterdir() if _is_delta_table_dir(p)]
    dir_by_key = {p.name.lower(): p for p in table_dirs}
    tables = sorted(dir_by_key.keys())

    def _normalize_table_key(name: str) -> str:
        name = (name or "").strip()
        if "." in name:
            name = name.split(".", 1)[-1]
        return name.lower()

    def load_table(table_name: str) -> pd.DataFrame:
        key = _normalize_table_key(table_name)
        table_dir = dir_by_key.get(key)

        if table_dir is None:
            # last resort: scan (covers unexpected casing / odd characters)
            for p in table_dirs:
                if p.name.lower() == key:
                    table_dir = p
                    break

        if table_dir is None or not _is_delta_table_dir(table_dir):
            log.warning(
                f"[discover_lake] Table not found or not a delta table: requested={table_name!r} "
                f"(key={key!r}) under root={lake_root}"
            )
            return pd.DataFrame()

        cols = _schema_from_delta_log(table_dir)
        if cols is None:
            cols = _fallback_schema_from_parquet(table_dir)

        if not cols:
            return pd.DataFrame()

        return pd.DataFrame(columns=cols)

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
        STAGING_SCHEMA: STAGING_BASE_PATH,
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
    attrs = [a for a in attributes if a not in business_keys]
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
        src_tbl = str(table_name).lower()
        lines.append("from " + jinja_source(src_name, src_tbl))
        lines.append("group by " + ", ".join(business_keys))

    else:  # sat
        # keys + attributes + hashdiff + audit
        select_cols = []
        for c in business_keys + attrs:
            if c not in select_cols:
                select_cols.append(c)
        for c in select_cols:
            lines.append(f"    {c},")
        # IMPORTANT: hashdiff should use attrs (attributes excluding business keys)
        if attrs:
            attrs_expr = ", ".join("coalesce(cast(" + c + " as string), '')" for c in attrs)
            lines.append("    sha2(concat_ws('||', " + attrs_expr + "), 256) as hashdiff,")
        else:
            lines.append("    sha2('', 256) as hashdiff,")
        lines.append("    current_timestamp() as load_datetime,")
        lines.append("    '" + table_name + "' as record_source")
        src_tbl = str(table_name).lower()
        lines.append("from " + jinja_source(src_name, src_tbl))

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
        log.info(f"[GEN] Generated/updated DBT JSON model for {model_name} (from lake table {table_name})")


def generate_schema_yml(table_names, output_path=None):
    table_names = list(table_names or [])

    if output_path is None:
        output_path = os.path.join(os.path.dirname(__file__), "models", "schema.yml")

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
    import os, json, shutil
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

    # Be tolerant to different callback signatures from bronze_ingestor
    def _after_write(*args, **kwargs):
        # Accept (touched,) or (touched, batch_id, counts)
        if not args:
            return
        written_tables = args[0] or []
        if not written_tables:
            return

        nonlocal table_to_models

        # Refresh mapping if we see tables we don't know yet (models are generated at runtime).
        if any(tbl not in table_to_models for tbl in written_tables):
            table_to_models = get_existing_model_tables()

        models = set()
        for tbl in written_tables:
            for m in table_to_models.get(tbl, []):
                models.add(m)

        if models:
            log.info("[DBT-QUEUE] epoch models=%s", sorted(models))
            queue_dbt_models(models)

    # New datasets would be dropped, producing empty micro-batches and E2E timeouts.
    allowed_tables = None

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
        # Create the stop event early so background workers can run during longer bootstrap phases.
        stop_event = RUN.stop_event or threading.Event()
        RUN.stop_event = stop_event

        # Determine processing mode early (bulk runs must not start long-lived background threads).
        processing_mode = (PROCESSING_MODE or "streaming").lower()
        if processing_mode not in {"streaming", "bulk"}:
            log.warning(f"[MODE] Unknown PROCESSING={PROCESSING_MODE} -> defaulting to 'streaming'")
            processing_mode = "streaming"

        # -------- Phase 0: Discover lake + bootstrap Bronze/DBT scaffolding --------
        lake_tables, load_table = discover_lake()
        bootstrap_bronze(lake_tables, load_table)  # precreate empty bronze tables (DDL)

        # Start CDC early, but ONLY after Kafka/topic is reachable to avoid long producer-blocking.
        # Skip full-load for tables present at discovery; initial-load path establishes their watermarks.
        if processing_mode == "streaming" and (RUN.cdc_thread is None or not RUN.cdc_thread.is_alive()):
            cdc_skip_full_load_tables = set(lake_tables)

            def _early_cdc_loop():
                backoff = 2.0
                while not stop_event.is_set():
                    try:
                        # Ensure topic + broker are ready BEFORE starting the infinite CDC loop.
                        check_and_create_topic()
                        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, timeout_sec=60)

                        # Now run CDC loop (runs until stop_event is set)
                        try:
                            cdc_producer_insert_only(
                                stop_event=stop_event,
                                skip_full_load_tables=cdc_skip_full_load_tables,
                            )
                        except TypeError:
                            # Backwards compatibility if signature doesn't include skip_full_load_tables
                            cdc_producer_insert_only(stop_event=stop_event)
                        return
                    except Exception as exc:
                        log.warning(
                            f"[CDC Producer] Early-start loop error: {exc}; retrying in {backoff:.1f}s"
                        )
                        try:
                            time.sleep(backoff)
                        except Exception:
                            pass

            cdc_thread = threading.Thread(target=_early_cdc_loop, daemon=False, name="cdc-insert-only")
            RUN.cdc_thread = cdc_thread
            cdc_thread.start()

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

        # Start the debounced DBT runner (coalesces per-batch model requests)
        start_dbt_debouncer()

        if models_to_run:
            # Queue instead of blocking startup; debouncer will run them.
            initial = sorted(models_to_run)
            log.info("[DBT] Queueing initial models for debounced run (count=%s).", len(initial))
            queue_dbt_models(initial)
        else:
            log.info("[DBT] No eligible models to run (or dbt missing).")


        # -------- Phase 1: Kafka readiness + topic ensure --------
        check_and_create_topic()  # make sure topic exists before streams/producers
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, timeout_sec=60)

        # -------- Phase 2: Stream up (consumer) --------
        # IMPORTANT: Do NOT overwrite stop_event here (that breaks early CDC + graceful shutdown).
        query = None
        if processing_mode == "streaming":
            query = streaming_dv_consumer_and_dbt(models_to_run)
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
        if processing_mode == "bulk":
            log.info("[PROCESSING-MODE] BULK: producing once for all lake tables and exiting")
            produced_map = produce_tables_once(sorted(lake_tables))
            wait_for_kafka_increase(sum(produced_map.values()), timeout_sec=60)
            return
        else:
            # 1) Take Kafka baseline BEFORE producing anything
            from utils.helper_service_ready import kafka_total_end, wait_for_kafka_total_at_least
            base_total = kafka_total_end(
                bootstrap=KAFKA_BOOTSTRAP_SERVERS,
                topic=os.getenv("KAFKA_TOPIC", "lake_stream"),
            )
            log.info(
                "[ASSERT][KAFKA_OFFSETS][BASE] bootstrap=%s topic=%s base_total=%s",
                KAFKA_BOOTSTRAP_SERVERS, os.getenv("KAFKA_TOPIC", "lake_stream"), base_total
            )

            produced_once = set()
            produced_total = 0

            if tables_needing_initial_load:
                todo = sorted(list(tables_needing_initial_load))
                log.info("[INITIAL LOAD] Producing full load for tables: %s", todo)
                produced_map = produce_tables_once(todo) or {}
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

            # 3) Gate on Spark seeing the growth
            from utils.helper_service_ready import wait_for_stream_offset_growth

            # Avoid hard dependency on any helper that may not exist; prefer active query.
            q = query
            try:
                helper = globals().get("get_active_stream_query_by_name")
                if callable(helper):
                    q = helper("lake_stream-generic-ingestor") or query
            except Exception:
                q = query

            if q:
                wait_for_stream_offset_growth(q, produced_total=produced_total, base_total=base_total, timeout_sec=60)
                log.info("[STREAM][status] isActive=%s", q.isActive)
                lp = q.lastProgress or {}
                try:
                    log.info("[STREAM][source-desc] %s", (lp.get("sources", [{}])[0].get("description")))
                except Exception:
                    pass

        # -------- Phase 4: Continuous CDC producer (insert-only) --------
        # IMPORTANT: Do not start a second CDC thread if early CDC is already running.
        if RUN.cdc_thread is None or not RUN.cdc_thread.is_alive():
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
                query.awaitTermination()
            else:
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
    from dv_grpc_service import serve as serve_vault_grpc

    # Start Vault gRPC server in a background daemon thread
    VAULT_GRPC_THREAD = threading.Thread(
        target=serve_vault_grpc,
        args=(VAULT_GRPC_STOP_EVENT,),
        daemon=True,
        name="vault-grpc-server",
    )
    VAULT_GRPC_THREAD.start()
    log.info("Vault gRPC server thread started.")

    # Start the existing orchestrator / streaming logic
    main()
