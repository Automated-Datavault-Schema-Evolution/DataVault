import os
import time

from logger import log

from config import CONTROL_DIR, MAINTENANCE_FLAG, MAINTENANCE_STATUS


def _write_status(txt: str):
    os.makedirs(CONTROL_DIR, exist_ok=True)
    with open(MAINTENANCE_STATUS, "w") as f:
        f.write(txt)


def _read_status() -> str:
    try:
        with open(MAINTENANCE_STATUS, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        return ""


def stop_streaming_query(q) -> None:
    if not q:
        return
    try:
        if q.isActive:
            log.info(f'[DVH_UTILS][STREAM][control] stopping streaming query {getattr(q, "name", q.id)}')
            q.stop()  # graceful: finishes current micro-batch
            # wait until inactive
            t0 = time.time()
            while q.isActive and (time.time() - t0) < 120:
                time.sleep(0.5)
    except Exception as e:
        log.debug(f"[DVH_UTILS][STREAM][control] stop skipped: {e}")


def maintenance_watchdog(query_holder, start_fn, start_args):
    """
    Watches for MAINTENANCE_FLAG file.
    When present: stop stream (consumer) so producers build Kafka backlog.
    When removed: restart stream and continue.
    """
    os.makedirs(CONTROL_DIR, exist_ok=True)
    _write_status("RUNNING")
    while True:
        try:
            if os.path.exists(MAINTENANCE_FLAG):
                # pause once
                if _read_status() != "PAUSED":
                    stop_streaming_query(query_holder.get("q"))
                    query_holder["q"] = None
                    _write_status("PAUSED")
                    log.info("[DVH_UTILS][MAINT] Stream paused; safe to prune Bronze tables now.")
                # wait here until flag removed
                time.sleep(1.0)
                continue

            # flag not present -> ensure running
            if _read_status() != "RUNNING":
                # restart stream
                log.info("[DVH_UTILS][MAINT] Resuming streaming consumer.")
                query_holder["q"] = start_fn(*start_args)
                _write_status("RUNNING")
            time.sleep(2.0)
        except Exception as e:
            log.debug(f"[DVH_UTILS][MAINT] watchdog loop: {e}")
            time.sleep(2.0)
