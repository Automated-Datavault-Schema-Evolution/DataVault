import threading
import time

from pyspark.sql.streaming import StreamingQueryListener
from logger import log

def log_progress_periodically(q, interval=30):
    def _loop():
        while q.isActive:
            p = q.lastProgress
            if p:  # None before first batch
                log.info("[STREAM][last] name=%s batchId=%s numIn=%s irps=%s prps=%s",
                         p["name"], p["batchId"], p["numInputRows"],
                         p.get("inputRowsPerSecond"), p.get("processedRowsPerSecond"))
            time.sleep(interval)
    threading.Thread(target=_loop, name="progress-logger", daemon=True).start()

class PerfListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        log.info(f"[STREAM] started id={event.id} runId={event.runId} name={event.name}")

    def onQueryProgress(self, event):
        p = event.progress  # StreamingQueryProgress
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
        add_ms  = int(dur.get("addBatch", 0))

        log.info(
            "[STREAM][perf] name=%s batchId=%s numIn=%s irps=%.2f prps=%.2f trigMs=%s addMs=%s stateOps=%s",
            p.name, p.batchId, p.numInputRows, irps, prps, trig_ms, add_ms, len(getattr(p, "stateOperators", []) or []),
        )

        # If you want a JSON line for external aggregation:
        log.debug("[STREAM][progress-json] %s", p.json)  # one JSON per batch

    def onQueryTerminated(self, event):
        log.warning(f"[STREAM] terminated id={event.id} runId={event.runId} "
                    f"exception={getattr(event, 'exception', None)}")