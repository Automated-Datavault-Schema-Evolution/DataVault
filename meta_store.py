import datetime
import os
import uuid

import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool
import json

from logger import log
from config import (
    LAKE_TYPE,
    METADATA_LINEAGE_PATH,
    METADATA_METADATA_PATH,
    METASTORE_DB_HOST,
    METASTORE_DB_PORT,
    METASTORE_DB,
    METASTORE_DB_USER,
    METASTORE_DB_PASSWORD,
    METASTORE_DB_SCHEMA,
)

# Global connection pool for the metastore
METASTORE_POOL = None
METASTORE_POOL_MIN = int(os.getenv("METASTORE_POOL_MIN", 1))
METASTORE_POOL_MAX = int(os.getenv("METASTORE_POOL_MAX", 5))



def _append_parquet(row, path):
    if os.path.exists(path):
        df = pd.read_parquet(path)
        df = pd.concat([df, row], ignore_index=True)
    else:
        df = row
    df.to_parquet(path, index=False)

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
                cur.execute(
                    sql.SQL("CREATE DATABASE {}" ).format(
                        sql.Identifier(METASTORE_DB)
                    )
                )
            log.info(f"Metastore database '{METASTORE_DB}' created.")
        except Exception as create_exc:
            log.error(
                f"Failed creating metastore database '{METASTORE_DB}': {create_exc}"
            )
            raise
        finally:
            if bootstrap_conn:
                bootstrap_conn.close()


def init_metastore_pool(minconn=None, maxconn=None):
    """Initialize the connection pool for the metastore."""
    global METASTORE_POOL
    if minconn is None:
        minconn = METASTORE_POOL_MIN
    if maxconn is None:
        maxconn = METASTORE_POOL_MAX

    if METASTORE_POOL is None:
        _ensure_metastore_db()
        try:
            METASTORE_POOL = SimpleConnectionPool(
                minconn,
                maxconn,
                host=METASTORE_DB_HOST,
                port=METASTORE_DB_PORT,
                dbname=METASTORE_DB,
                user=METASTORE_DB_USER,
                password=METASTORE_DB_PASSWORD,
            )
            log.info(
                f"Metastore connection pool created (min={minconn}, max={maxconn})."
            )
        except Exception as e:
            log.error(f"Error establishing metastore connection pool: {e}")
            raise
    else:
        log.debug("Reusing existing metastore connection pool.")
    return METASTORE_POOL


def get_metastore_connection():
    """Get a connection from the metastore pool."""
    pool = init_metastore_pool()
    try:
        conn = pool.getconn()
        if conn.closed:
            log.warning("Received closed connection from pool; replacing it.")
            pool.putconn(conn, close=True)
            conn = pool.getconn()
        log.debug("Acquired connection from metastore pool.")
        return conn
    except Exception as e:
        msg = str(e)
        if "connection pool exhausted" in msg.lower():
            new_max = pool.maxconn + 5
            log.warning(
                f"Metastore connection pool exhausted. Expanding pool to {new_max}."
            )
            try:
                pool.closeall()
            except Exception:
                pass
            init_metastore_pool(pool.minconn, new_max)
            pool = METASTORE_POOL
            return pool.getconn()
        log.error(f"Error getting metastore connection: {e}")
        raise


def release_metastore_connection(conn):
    """Return a connection back to the metastore pool."""
    pool = init_metastore_pool()
    try:
        if conn.closed:
            pool.putconn(conn, close=True)
            log.debug("Closed dead metastore connection from pool.")
        else:
            pool.putconn(conn)
            log.debug("Released metastore connection back to pool.")
    except Exception as e:
        log.error(f"Error releasing metastore connection: {e}")


def _append_db(data, table):
    conn = None
    try:
        conn = get_metastore_connection()
        with conn.cursor() as cur:
            # Ensure schema and target table exist. Each record is stored as
            # JSONB to keep the schema flexible.
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(
                    sql.Identifier(METASTORE_DB_SCHEMA)
                )
            )
            cur.execute(
                sql.SQL(
                    """
                    CREATE TABLE IF NOT EXISTS {}.{} (
                        id SERIAL PRIMARY KEY,
                        payload JSONB,
                        inserted_at TIMESTAMPTZ DEFAULT NOW()
                    )
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
        log.error(f"Error appending to metastore table '{table}': {e}")
    finally:
        if conn:
            release_metastore_connection(conn)

def write_lineage(metadata_dict):
    """Append lineage information to the lineage metastore."""
    metadata_dict.setdefault("lineage_id", str(uuid.uuid4()))
    metadata_dict.setdefault("logged_at", datetime.utcnow().isoformat())

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