"""Shared runtime state for graceful shutdown."""

import threading
from types import SimpleNamespace

RUN = SimpleNamespace(stop_event=None, query=None, cdc_thread=None)

VAULT_GRPC_STOP_EVENT = threading.Event()
VAULT_GRPC_THREAD: threading.Thread | None = None
