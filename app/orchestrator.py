"""Orchestrator entrypoint for DataVaultHandler.

Contains the original main() logic extracted from main.py.
"""

import fcntl
import os
import threading
import time

from logger import log

from core.runtime import RUN
from core.dbt_debouncer import start_dbt_debouncer, stop_dbt_debouncer, queue_dbt_models
from core.lake_discovery import discover_lake
from helper.dbt_models_helper import (
    ensure_dbt_models_for_lake,
    get_existing_model_tables,
    write_json_model_file,
    generate_schema_yml,
)
from helper.dbt_profiles_helper import ensure_profiles_dir, ensure_real_profiles
from helper.dbt_runner import run_dbt_models
from helper.hive_schema_helper import ensure_database_schema
from helper.hive_introspection import get_raw_vault_tables
from core.streaming_pipeline import streaming_dv_consumer_and_dbt
from core.bootstrap import bootstrap_bronze
from helper.kafka_helper import cdc_producer_insert_only
from cdc_kafka_producer import check_and_create_topic, produce_tables_once
from helper.spark_helper import ensure_spark_warehouse_dir
from utils.helper_service_ready import wait_for_lake, wait_for_kafka, wait_for_kafka_increase
from utils.maintenance.helper_maintenance import maintenance_watchdog
from utils.performance_logger import PerfListener, log_progress_periodically

from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA, DBT_MODELS_JSON_DIR, THRIFT_HOST, THRIFT_PORT, DBT_MODELS_SQL_DIR,
    KAFKA_STARTING_OFFSETS, KAFKA_GROUP_ID, STAGING_SCHEMA, RAW_VAULT_SCHEMA, PROCESSING_MODE,
    KAFKA_MAX_OFFSETS_PER_TRIGGER,
)

from app.shutdown import _graceful_shutdown

class _SingletonRunLock():
    """
    Best-effort singelton run guard to avoid two orchestrators booting oncurrently.
    """

    def __init__(self, path="/tmp/dv_orchestrator.lock"):
        self.path = path
        self._fh = None

    def __enter__(self):
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._fh = open(self.path, "w")
        fcntl.flock(self._fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
        self._fh.write(str(os.getpid()))
        self._fh.flush()
        return self

    def __exit__(self, *exc):
        try:
            fcntl.flock(self._fh, fcntl.LOCK_UN)
            self._fh.close()
        except Exception:
            pass


def main():
    os.makedirs(DBT_MODELS_JSON_DIR, exist_ok=True)
    ensure_spark_warehouse_dir()
    ensure_profiles_dir()
    ensure_database_schema()

    # wait for lake readiness before discovery
    wait_for_lake(timeout_sec=60)

    with _SingletonRunLock():
        # Create the stop event early so background workers can run during longer bootstrap phases.
        stop_event = RUN.stop_event or threading.Event()
        RUN.stop_event = stop_event

        # Determine processing mode early (bulk runs must not start long-lived background threads).
        processing_mode = (PROCESSING_MODE or "streaming").lower()
        if processing_mode not in {"streaming", "bulk"}:
            log.warning(f"[DVH_APP][MODE] Unknown PROCESSING={PROCESSING_MODE} -> defaulting to 'streaming'")
            processing_mode = "streaming"

        # -------- Phase 0: Discover lake + bootstrap Bronze/DBT scaffolding --------
        lake_tables, load_table = discover_lake()
        bootstrap_bronze(lake_tables, load_table)  # precreate empty bronze tables (DDL)

        # Start CDC early, but ONLY after Kafka/topic is reachable to avoid long producer-blocking.
        # Skip full-load for tables present at discovery; initial-load path establishes their watermarks.
        if processing_mode == "streaming" and (RUN.cdc_thread is None or not RUN.cdc_thread.is_alive()):
            cdc_skip_full_load_tables = set(lake_tables)

            def _early_cdc_loop():
                backoff = 2.0
                while not stop_event.is_set():
                    try:
                        # Ensure topic + broker are ready BEFORE starting the infinite CDC loop.
                        check_and_create_topic()
                        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, timeout_sec=60)

                        # Now run CDC loop (runs until stop_event is set)
                        try:
                            cdc_producer_insert_only(
                                stop_event=stop_event,
                                skip_full_load_tables=cdc_skip_full_load_tables,
                            )
                        except TypeError:
                            # Backwards compatibility if signature doesn't include skip_full_load_tables
                            cdc_producer_insert_only(stop_event=stop_event)
                        return
                    except Exception as exc:
                        log.warning(f'[DVH_APP][CDC Producer] Early-start loop error: {exc}; retrying in {backoff:.1f}s')
                        try:
                            time.sleep(backoff)
                        except Exception:
                            pass

            cdc_thread = threading.Thread(target=_early_cdc_loop, daemon=False, name="cdc-insert-only")
            RUN.cdc_thread = cdc_thread
            cdc_thread.start()

        generate_schema_yml(lake_tables)
        existing_models = get_existing_model_tables()
        vault_tables = get_raw_vault_tables()

        # Determine which tables are new (no model yet) and which vault objects are missing
        tables_without_models = [t for t in lake_tables if t not in existing_models]
        models_to_run = set()
        tables_needing_initial_load = set()

        if tables_without_models:
            # Generate models for new tables
            new_models = ensure_dbt_models_for_lake(tables_without_models, load_table)
            models_to_run.update(new_models)
            # all new tables will need an initial full load after objects are created
            tables_needing_initial_load.update(tables_without_models)

        # If any model exists but the physical table is missing in the vault, we must create it
        for table, model_names in existing_models.items():
            for m in model_names:
                if m not in vault_tables:
                    models_to_run.add(m)
                    # this lake table is missing at least one DV object -> full load needed
                    tables_needing_initial_load.add(table)

        # Start the debounced DBT runner (coalesces per-batch model requests)
        start_dbt_debouncer()

        if models_to_run:
            # Queue instead of blocking startup; debouncer will run them.
            initial = sorted(models_to_run)
            log.info(f'[DVH_APP][DBT] Queueing initial models for debounced run (count={len(initial):}).')
            queue_dbt_models(initial)
        else:
            log.info('[DVH_APP][DBT] No eligible models to run (or dbt missing).')


        # -------- Phase 1: Kafka readiness + topic ensure --------
        check_and_create_topic()  # make sure topic exists before streams/producers
        wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, timeout_sec=60)

        # -------- Phase 2: Stream up (consumer) --------
        # IMPORTANT: Do NOT overwrite stop_event here (that breaks early CDC + graceful shutdown).
        query = None
        if processing_mode == "streaming":
            query = streaming_dv_consumer_and_dbt(models_to_run)
            RUN.query = query

            # Small settle time so Spark attaches before initial production
            try:
                time.sleep(1)
            except Exception:
                pass

            # Maintenance watchdog (pause/resume around daily prune)
            threading.Thread(
                target=maintenance_watchdog,
                args=(query, streaming_dv_consumer_and_dbt, (models_to_run,)),
                daemon=True,
                name="Maintenance Watchdog",
            ).start()

        # -------- Phase 3: Initial full load (backlog) --------
        if processing_mode == "bulk":
            log.info('[DVH_APP][PROCESSING-MODE] BULK: producing once for all lake tables and exiting')
            produced_map = produce_tables_once(sorted(lake_tables))
            wait_for_kafka_increase(sum(produced_map.values()), timeout_sec=60)
            return
        else:
            # 1) Take Kafka baseline BEFORE producing anything
            from utils.helper_service_ready import kafka_total_end, wait_for_kafka_total_at_least
            base_total = kafka_total_end(
                bootstrap=KAFKA_BOOTSTRAP_SERVERS,
                topic=os.getenv("KAFKA_TOPIC", "lake_stream"),
            )
            log.info(f"[DVH_APP][ASSERT][KAFKA_OFFSETS][BASE] bootstrap={KAFKA_BOOTSTRAP_SERVERS:} topic={os.getenv('KAFKA_TOPIC', 'lake_stream'):} base_total={base_total:}")

            produced_once = set()
            produced_total = 0

            if tables_needing_initial_load:
                todo = sorted(list(tables_needing_initial_load))
                log.info(f'[DVH_APP][INITIAL LOAD] Producing full load for tables: {todo:}')
                produced_map = produce_tables_once(todo) or {}
                produced_total += sum(produced_map.values())
                produced_once |= set(todo)

            remaining = [t for t in lake_tables if t not in produced_once]
            if remaining:
                log.info(f'[DVH_APP][INITIAL LOAD] Producing full load for remaining tables: {remaining:}')
                produced_map = produce_tables_once(remaining) or {}
                produced_total += sum(produced_map.values())

            # 2) Gate on Kafka reaching base + produced_total
            target_total = base_total + produced_total
            wait_for_kafka_total_at_least(
                min_total=target_total,
                timeout_sec=60,
                bootstrap=KAFKA_BOOTSTRAP_SERVERS,
                topic=os.getenv("KAFKA_TOPIC", "lake_stream"),
            )

            # 3) Gate on Spark seeing the growth
            from utils.helper_service_ready import wait_for_stream_offset_growth

            # Avoid hard dependency on any helper that may not exist; prefer active query.
            q = query
            try:
                helper = globals().get("get_active_stream_query_by_name")
                if callable(helper):
                    q = helper("lake_stream-generic-ingestor") or query
            except Exception:
                q = query

            if q:
                wait_for_stream_offset_growth(q, produced_total=produced_total, base_total=base_total, timeout_sec=60)
                log.info(f'[DVH_APP][STREAM][status] isActive={q.isActive:}')
                lp = q.lastProgress or {}
                try:
                    log.info(f"[DVH_APP][STREAM][source-desc] {lp.get('sources', [{}])[0].get('description'):}")
                except Exception:
                    pass

        # -------- Phase 4: Continuous CDC producer (insert-only) --------
        # IMPORTANT: Do not start a second CDC thread if early CDC is already running.
        if RUN.cdc_thread is None or not RUN.cdc_thread.is_alive():
            def _cdc_loop():
                try:
                    cdc_producer_insert_only(stop_event=stop_event)
                except TypeError:
                    cdc_producer_insert_only()

            cdc_thread = threading.Thread(target=_cdc_loop, daemon=False, name="cdc-insert-only")
            RUN.cdc_thread = cdc_thread
            cdc_thread.start()

        # -------- Phase 5: Lifecycle / graceful shutdown --------
        try:
            if query is not None:
                query.awaitTermination()
            else:
                while not stop_event.is_set():
                    time.sleep(1)
        except KeyboardInterrupt:
            _graceful_shutdown()
        finally:
            try:
                stop_dbt_debouncer()
            finally:
                _graceful_shutdown()

