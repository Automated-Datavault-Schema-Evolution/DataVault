import json
import os
import threading
import time
import uuid
from datetime import datetime

import pandas as pd
import psycopg2
from logger import log
from psycopg2 import sql
from psycopg2.pool import PoolError, ThreadedConnectionPool

from config import (
    LAKE_TYPE,
    METADATA_LINEAGE_PATH,
    METADATA_METADATA_PATH,
    METASTORE_DB_HOST,
    METASTORE_DB_PORT,
    METASTORE_DB,
    METASTORE_DB_USER,
    METASTORE_DB_PASSWORD,
    METASTORE_DB_SCHEMA, POSTGRES_POOL_MIN, POSTGRES_POOL_MAX,
)

# Global connection pool for the metastore
METASTORE_POOL = None

# Acquire behavior: bounded wait instead of closing the pool under load
METASTORE_POOL_ACQUIRE_TIMEOUT_S = float(os.getenv("METASTORE_POOL_ACQUIRE_TIMEOUT_S", "30"))
METASTORE_POOL_ACQUIRE_RETRY_S = float(os.getenv("METASTORE_POOL_ACQUIRE_RETRY_S", "0.2"))

_POOL_LOCK = threading.Lock()
_PARQUET_APPEND_LOCK = threading.Lock()

def _append_parquet(row, path):
    """
    Append a row (DataFrame) to a parquet file at `path`.

    Minimal hardening:
      - Ensure parent directory exists (fixes: Cannot save file into a non-existent directory: 'meta')
      - Serialize read/concat/write to avoid concurrent corruption under gRPC load
      - Use atomic replace to avoid partially-written parquet files
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

    with _PARQUET_APPEND_LOCK:
        if os.path.exists(path):
            try:
                df_existing = pd.read_parquet(path)
                df = pd.concat([df_existing, row], ignore_index=True)
            except Exception as exc:
                # If file is corrupt/half-written, overwrite with the new row rather than failing every retry
                log.warning(f"[META] Failed reading existing parquet {path!r}: {exc}; overwriting.")
                df = row
        else:
            df = row

        tmp_path = f"{path}.tmp-{uuid.uuid4().hex}"
        try:
            df.to_parquet(tmp_path, index=False)
            os.replace(tmp_path, path)  # atomic on POSIX
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
            except Exception:
                pass


def _ensure_metastore_db():
    """Ensure that the metastore database exists."""
    try:
        conn = psycopg2.connect(
            host=METASTORE_DB_HOST,
            port=METASTORE_DB_PORT,
            dbname=METASTORE_DB,
            user=METASTORE_DB_USER,
            password=METASTORE_DB_PASSWORD,
        )
        conn.close()
        log.debug("Metastore database exists.")
        return
    except psycopg2.OperationalError as exc:
        if "does not exist" not in str(exc):
            log.error(f"Error connecting to metastore database: {exc}")
            raise

    log.info(f"Metastore database '{METASTORE_DB}' missing. Creating it.")
    bootstrap_conn = None
    try:
        bootstrap_conn = psycopg2.connect(
            host=METASTORE_DB_HOST,
            port=METASTORE_DB_PORT,
            dbname="postgres",
            user=METASTORE_DB_USER,
            password=METASTORE_DB_PASSWORD,
        )
        bootstrap_conn.autocommit = True
        with bootstrap_conn.cursor() as cur:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(METASTORE_DB)))
        log.info(f"Metastore database '{METASTORE_DB}' created.")
    except Exception as create_exc:
        log.error(f"Failed creating metastore database '{METASTORE_DB}': {create_exc}")
        raise
    finally:
        if bootstrap_conn:
            bootstrap_conn.close()


def init_metastore_pool(minconn=None, maxconn=None):
    """
    Initialize the connection pool for the metastore.

    Real fix:
      - Use ThreadedConnectionPool (thread-safe).
      - Never close/recreate the pool during normal operation (that breaks in-flight requests).
    """
    global METASTORE_POOL

    if minconn is None:
        minconn = POSTGRES_POOL_MIN
    if maxconn is None:
        maxconn = POSTGRES_POOL_MAX

    with _POOL_LOCK:
        if METASTORE_POOL is None or getattr(METASTORE_POOL, "closed", False):
            _ensure_metastore_db()
            try:
                METASTORE_POOL = ThreadedConnectionPool(
                    minconn,
                    maxconn,
                    host=METASTORE_DB_HOST,
                    port=METASTORE_DB_PORT,
                    dbname=METASTORE_DB,
                    user=METASTORE_DB_USER,
                    password=METASTORE_DB_PASSWORD,
                )
                log.info(f"Metastore connection pool created (min={minconn}, max={maxconn}).")
            except Exception as e:
                log.error(f"Error establishing metastore connection pool: {e}")
                raise
        else:
            log.debug("Reusing existing metastore connection pool.")

        return METASTORE_POOL


def get_metastore_connection():
    """
    Get a connection from the metastore pool.

    Real fix:
      - Do NOT closeall()/recreate the pool on exhaustion.
      - On exhaustion, wait/retry for a released connection (bounded timeout).
    """
    pool = init_metastore_pool()
    deadline = time.time() + METASTORE_POOL_ACQUIRE_TIMEOUT_S
    last_err = None

    while time.time() < deadline:
        try:
            conn = pool.getconn()
            if conn is None:
                raise RuntimeError("Metastore pool returned None connection")

            # Replace dead connections immediately
            if getattr(conn, "closed", 0):
                log.warning("Received closed connection from pool; replacing it.")
                try:
                    pool.putconn(conn, close=True)
                except Exception:
                    pass
                continue

            log.debug("Acquired connection from metastore pool.")
            return conn


        except PoolError as e:
            last_err = e
            if "closed" in str(e).lower():
                log.error("Metastore pool is closed; recreating pool.")
                with _POOL_LOCK:
                    global METASTORE_POOL
                    METASTORE_POOL = None
                pool = init_metastore_pool()
                time.sleep(METASTORE_POOL_ACQUIRE_RETRY_S)
                continue
            time.sleep(METASTORE_POOL_ACQUIRE_RETRY_S)
            continue

        except Exception as e:
            log.error(f"Error getting metastore connection: {e}")
            raise

    msg = (
        "Metastore connection acquisition timed out after "
        f"{METASTORE_POOL_ACQUIRE_TIMEOUT_S:.1f}s "
        f"(min={getattr(pool, 'minconn', '?')}, max={getattr(pool, 'maxconn', '?')}). "
        "Increase METASTORE_POOL_MAX or reduce concurrency."
    )
    log.error(msg)
    if last_err is not None:
        raise last_err
    raise TimeoutError(msg)


def release_metastore_connection(conn):
    """Return a connection back to the metastore pool."""
    if conn is None:
        return

    pool = init_metastore_pool()
    try:
        if getattr(conn, "closed", 0):
            pool.putconn(conn, close=True)
            log.debug("Closed dead metastore connection from pool.")
        else:
            pool.putconn(conn)
            log.debug("Released metastore connection back to pool.")
    except PoolError as e:
        # Pool unexpectedly closed: do not crash; close the connection
        log.error(f"Metastore pool error while releasing connection: {e}. Closing connection.")
        try:
            conn.close()
        except Exception:
            pass
    except Exception as e:
        log.error(f"Error releasing metastore connection: {e}")
        try:
            conn.close()
        except Exception:
            pass


def _append_db(data, table):
    conn = None
    try:
        conn = get_metastore_connection()
        with conn.cursor() as cur:
            # Ensure schema and target table exist. Each record is stored as JSONB.
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(METASTORE_DB_SCHEMA)))
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        id SERIAL PRIMARY KEY,
                        payload JSONB,
                        inserted_at TIMESTAMPTZ DEFAULT NOW()
                    );
                    """
                ).format(sql.Identifier(METASTORE_DB_SCHEMA), sql.Identifier(table))
            )
            cur.execute(
                sql.SQL("INSERT INTO {}.{} (payload) VALUES (%s)").format(
                    sql.Identifier(METASTORE_DB_SCHEMA), sql.Identifier(table)
                ),
                [json.dumps(data)],
            )
        conn.commit()
    except Exception as e:
        # IMPORTANT: rollback so we don't return an aborted connection to the pool
        try:
            if conn is not None:
                conn.rollback()
        except Exception:
            pass
        log.error(f"Error appending to metastore table '{table}': {e}")
        # Do not raise: metastore write failure should not crash ingestion
    finally:
        if conn:
            release_metastore_connection(conn)


def write_lineage(metadata_dict):
    """Append lineage information to the lineage metastore."""
    metadata_dict.setdefault("lineage_id", str(uuid.uuid4()))
    metadata_dict.setdefault("logged_at", datetime.now().isoformat())

    if LAKE_TYPE == "rdbms":
        _append_db(metadata_dict, "lineage")
    else:
        _append_parquet(pd.DataFrame([metadata_dict]), METADATA_LINEAGE_PATH)


def write_metadata(metadata_dict):
    """Append model metadata to the metadata metastore."""
    if LAKE_TYPE == "rdbms":
        _append_db(metadata_dict, "metadata")
    else:
        _append_parquet(pd.DataFrame([metadata_dict]), METADATA_METADATA_PATH)
