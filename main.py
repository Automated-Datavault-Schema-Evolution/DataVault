"""Thin entrypoint + re-exports for gRPC service compatibility."""

# Register shutdown handlers at import time (matches original behavior)
from app import shutdown as _shutdown  # noqa: F401

from core.lake_discovery import discover_lake
from helper.dbt_models_helper import write_json_model_file
from helper.dbt_runner import run_dbt_models
from core.dbt_debouncer import queue_dbt_models, start_dbt_debouncer, stop_dbt_debouncer


def main():
    from app.entrypoint import main as _main
    _main()


if __name__ == "__main__":
    main()
