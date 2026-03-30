"""Graceful shutdown handling.

Extracted from the original main.py without behavioral changes.
Handlers are registered at import time to match original behavior.
"""

import atexit
import signal
from logger import log

from core.runtime import RUN, VAULT_GRPC_STOP_EVENT, VAULT_GRPC_THREAD
from core.dbt_debouncer import stop_dbt_debouncer

def _graceful_shutdown(signum=None, frame=None):
    """Handle SIGTERM/SIGINT and atexit: stop CDC + drain/stop Spark cleanly."""
    try:
        log.info(f'[DVH_APP][SHUTDOWN] signal={signum} received, draining ......')
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
                log.warning(f'[DVH_APP][SHUTDOWN] query.stop() failed: {e}]')
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
