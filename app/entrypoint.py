"""Process entrypoint.

Matches original __main__ behavior from main.py.
"""

import threading
from logger import log

from core.runtime import VAULT_GRPC_STOP_EVENT
from app.orchestrator import main as orchestrator_main


def main():
    from dv_grpc_service import serve as serve_vault_grpc

    # Start Vault gRPC server in a background thread and stop it cooperatively on exit.
    from core import runtime as rt
    rt.VAULT_GRPC_THREAD = threading.Thread(
        target=serve_vault_grpc,
        args=(VAULT_GRPC_STOP_EVENT,),
        daemon=True,
        name="vault-grpc-server",
    )
    rt.VAULT_GRPC_THREAD.start()
    log.info("Vault gRPC server thread started.")

    try:
        orchestrator_main()
    finally:
        try:
            VAULT_GRPC_STOP_EVENT.set()
        except Exception:
            pass
        try:
            if rt.VAULT_GRPC_THREAD is not None and rt.VAULT_GRPC_THREAD.is_alive():
                rt.VAULT_GRPC_THREAD.join(timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
