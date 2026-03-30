"""dbt debouncer (queue + background worker).

Extracted from the original main.py without behavioral changes.
"""

import json
import os
import threading
import time
from typing import Iterable, Set, Optional

from logger import log

from helper.dbt_runner import run_dbt_models
from config import DBT_DEBOUNCE_SECONDS, DBT_MAX_MODELS_PER_RUN

_DBT_PENDING_LOCK = threading.Lock()
_DBT_WORKER_STOP = threading.Event()
_DBT_WAKE = threading.Event()
_DBT_WORKER_THREAD: Optional[threading.Thread] = None

_DBT_PENDING_MODELS: set[str] = set()
_DBT_PENDING_LOADED = False
_DBT_LAST_ENQUEUE_TS = 0.0

_DBT_PENDING_FILE = os.getenv("DBT_PENDING_MODELS_FILE", "/data/state/dbt_pending_models.json")

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
        log.warning(f'[DVH_CORE][DBT-DEBOUNCER] Failed to load pending models from {_DBT_PENDING_FILE}: {exc}')

def _persist_pending_models_to_disk() -> None:
    """Atomically persist pending models."""
    try:
        os.makedirs(os.path.dirname(_DBT_PENDING_FILE), exist_ok=True)
        tmp = _DBT_PENDING_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(sorted(_DBT_PENDING_MODELS), f)
        os.replace(tmp, _DBT_PENDING_FILE)
    except Exception as exc:
        log.warning(f'[DVH_CORE][DBT-DEBOUNCER] Failed to persist pending models to {_DBT_PENDING_FILE}: {exc}')

def queue_dbt_models(models: Iterable[str]) -> None:
    """
    Collect models to run; actual run is done by the debouncer thread.

    Key properties:
      - Safe to call before the debouncer is started
      - Debounced: coalesces bursts of updates into a single dbt run
      - Persists pending set to survive restarts
    """
    global _DBT_LAST_ENQUEUE_TS

    models = [m for m in (models or []) if m and str(m).strip()]
    if not models:
        return

    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()
        before = len(_DBT_PENDING_MODELS)
        _DBT_PENDING_MODELS.update(str(m).strip() for m in models)

        # IMPORTANT: update the debounce timestamp even if the set didn't grow.
        # A single model may be updated multiple times (e.g. multi-column evolution),
        # and we must not start dbt until the last update has landed on disk.
        _DBT_LAST_ENQUEUE_TS = time.time()

        if len(_DBT_PENDING_MODELS) != before:
            _persist_pending_models_to_disk()

    # Wake worker so it can eventually run once the quiet window has elapsed.
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
    global _DBT_LAST_ENQUEUE_TS
    if not batch:
        return
    with _DBT_PENDING_LOCK:
        _load_pending_models_from_disk()
        _DBT_PENDING_MODELS.update(batch)
        _DBT_LAST_ENQUEUE_TS = time.time()
        _persist_pending_models_to_disk()
    _DBT_WAKE.set()

def _dbt_worker_loop(interval_seconds: int, max_models_per_run: int) -> None:
    """
    Background loop that runs dbt for accumulated models.
    - Wake-on-queue for fast reaction
    - Retry by re-queuing on failure
    """
    log.info(f'[DVH_CORE][DBT-DEBOUNCER] started: interval={interval_seconds:}s, max_models_per_run={max_models_per_run:}')

    try:
        interval_seconds = max(1, int(interval_seconds or 1))

        while not _DBT_WORKER_STOP.is_set():
            # Wait for either a wake signal or the periodic interval.
            _DBT_WAKE.wait(timeout=float(interval_seconds))
            _DBT_WAKE.clear()

            if _DBT_WORKER_STOP.is_set():
                break

            # Proper debounce: only run once we've had a quiet period with no new queue activity.
            # This avoids thrashing dbt on multi-operation plans (e.g. adding many columns),
            # and ensures model JSON/SQL files are stable before dbt reads them.
            while True:
                if _DBT_WORKER_STOP.is_set():
                    break

                with _DBT_PENDING_LOCK:
                    _load_pending_models_from_disk()
                    pending = bool(_DBT_PENDING_MODELS)
                    last_ts = float(_DBT_LAST_ENQUEUE_TS or 0.0)

                if not pending:
                    break

                quiet_for = time.time() - last_ts
                remaining = float(interval_seconds) - quiet_for
                if remaining <= 0:
                    break

                # Wait the remaining quiet window (or wake early if new models arrive).
                _DBT_WAKE.wait(timeout=remaining)
                _DBT_WAKE.clear()

            if _DBT_WORKER_STOP.is_set():
                break

            batch = _drain_models(max_models_per_run)
            if not batch:
                continue

            try:
                log.info(f'[DVH_CORE][DBT-DEBOUNCER] running (size={len(batch):}): {batch:}')
                run_dbt_models(batch)
            except Exception as exc:
                log.warning(f'[DVH_CORE][DBT-DEBOUNCER] dbt run failed; re-queueing batch. error={exc}')
                _requeue(batch)
                time.sleep(min(10.0, float(interval_seconds)))


    finally:
        log.info('[DVH_CORE][DBT-DEBOUNCER] stopped')

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


