"""Postgres helper functions (pool + connections).

Extracted from the original main.py without behavioral changes.
"""

import threading
import time
from logger import log
from psycopg2.pool import SimpleConnectionPool

from config import (
    RDBMS_HOST,
    RDBMS_PORT,
    RDBMS_DB,
    RDBMS_USER,
    RDBMS_PASSWORD,
    POSTGRES_POOL_MIN,
    POSTGRES_POOL_MAX,
)

PG_POOL = None
_PG_POOL_LOCK = threading.Lock()

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
        log.warning(f'PostgreSQL pool reset ({reason:}).')

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
                log.warning(f'Existing PostgreSQL pool unusable ({e:}). Recreating.')
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


