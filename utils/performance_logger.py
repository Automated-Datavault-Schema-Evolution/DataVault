import os
import threading
import time

from logger import log
from pyspark.sql.streaming import StreamingQueryListener

def log_progress_periodically(q, interval=30):
    try:
        interval = int(os.getenv("DVH_PROGRESS_LOG_INTERVAL_S", str(interval)))
    except Exception:
        interval = interval
    def _loop():
        while q.isActive:
            p = q.lastProgress
            if p:  # None before first batch
                log.info(f'[DVH_UTILS][STREAM][last] name={p["name"]} batchId={p["batchId"]} numIn={p["numInputRows"]} irps={p.get("inputRowsPerSecond")} prps={p.get("processedRowsPerSecond")}')
            time.sleep(interval)

    threading.Thread(target=_loop, name="progress-logger", daemon=True).start()


class PerfListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info(f"[DVH_UTILS][STREAM] started id={event.id} runId={event.runId} name={event.name}")

    def onQueryProgress(self, event):
        p = event.progress
        try:
            irps = float(p.inputRowsPerSecond)
        except Exception:
            irps = -1.0
        try:
            prps = float(p.processedRowsPerSecond)
        except Exception:
            prps = -1.0

        # Useful durations (ms) if available
        dur = getattr(p, "durationMs", {}) or {}
        trig_ms = int(dur.get("triggerExecution", 0))
        add_ms = int(dur.get("addBatch", 0))

        log.info(f'[DVH_UTILS][STREAM][perf] name={p.name} batchId={p.batchId} numIn={p.numInputRows} irps={irps} prps={prps} trigMs={trig_ms} addMs={add_ms} stateOps={len(getattr(p, "stateOperators", []) or [])}')
        log.debug(f"[DVH_UTILS][STREAM][progress-json] {p.json}")  # one JSON per batch

    def onQueryTerminated(self, event):
        log.warning(f"[DVH_UTILS][STREAM] terminated id={event.id} runId={event.runId} "
                    f"exception={getattr(event, 'exception', None)}")
