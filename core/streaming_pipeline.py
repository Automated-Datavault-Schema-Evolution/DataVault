"""Streaming pipeline (Kafka -> Bronze -> dbt).

Extracted from the original main.py without behavioral changes.
"""

from logger import log
from helper.spark_helper import get_spark_session, get_active_stream_query_by_name
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_STARTING_OFFSETS,
    KAFKA_GROUP_ID,
    KAFKA_MAX_OFFSETS_PER_TRIGGER,
    PROCESSING_MODE,
    LAKE_TYPE,
    STAGING_BASE_PATH,
    STREAM_TRIGGER,
)
from utils.bronze_ingestor import start_bronze_writer
from utils.schema_helpers import infer_schema_from_cdc_event
from helper.dbt_models_helper import get_existing_model_tables
from core.dbt_debouncer import queue_dbt_models
from utils.performance_logger import PerfListener, log_progress_periodically

def get_kafka_stream(spark, table_name, schema):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("cdc_modified_at", StringType())
    ])
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)  # earlist for first run, then checkpoint
        .option("groupIdPrefix", KAFKA_GROUP_ID)
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    df_data = df_table.select(from_json(col("json.payload"), schema).alias("data")).select("data.*")
    return df_data

def streaming_dv_consumer_and_dbt(models_to_run):
    """
    Generic, schema-late binding Kafka -> Bronze streaming consumer + dbt trigger.
    Writes bronze tables as Delta, auto-creating them, and triggers dbt per touched table.
    """
    import os, json, shutil
    from pyspark.sql import functions as F  # FIX: F used later
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType

    spark = get_spark_session("DataVault_Streaming_Consumer")
    try:
        spark.conf.set("spark.sql.sources.default", "delta")  # FIX: safer default
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    except Exception:
        pass

    try:
        spark.streams.addListener(PerfListener())
    except Exception as e:
        log.debug(f'PerfListener attach skipped: {e:}')

    try:
        spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
    except Exception:
        pass

    envelope_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("cdc_modified_at", StringType()),
    ])

    checkpoint_root = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints")
    checkpoint_dir = os.path.join(checkpoint_root, f"{KAFKA_TOPIC}_generic_v3")

    ## TODO: ONLY FOR TESTING, REMOVE BEFORE DEPLOYMENT
    if os.environ.get("STREAM_CHECKPOINT_RESET", "").lower() in {"1", "true", "yes"}:
        log.warning(f'[DVH_CORE][STREAM] Wiping checkpoint dir: {checkpoint_dir:}')
        shutil.rmtree(checkpoint_dir, ignore_errors=True)

    log.info(f'[DVH_CORE][STREAM][source] bootstrap={KAFKA_BOOTSTRAP_SERVERS:} topic={KAFKA_TOPIC:} startingOffsets=earliest')
    src = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.metadata.max.age.ms", "2000")
        .option("kafka.partition.discovery.interval.ms", "2000")
        .option("kafkaConsumer.pollTimeoutMs", "1000")
        .option("maxOffsetsPerTrigger", KAFKA_MAX_OFFSETS_PER_TRIGGER)
        .load()
        .select(from_json(col("value").cast("string"), envelope_schema).alias("json"))
        .where(col("json").isNotNull())
        .select(
            col("json.table").alias("table"),
            col("json.payload").alias("payload"),
            col("json.cdc_type").alias("cdc_type"),
            col("json.cdc_modified_at").alias("cdc_modified_at"),
        )
        .where(col("table").isNotNull())
    )

    table_to_models = get_existing_model_tables()

    def _infer_schema_from_batch(df_tbl):
        rows = df_tbl.select("payload").limit(1).collect()
        if not rows:
            return None
        try:
            obj = json.loads(rows[0]["payload"])
            return StructType([StructField(k, StringType(), True) for k in obj.keys()])
        except Exception as e:
            log.debug(f'Inline schema inference failed: {e:}')
            return None

    def _process_table(batch_df, tbl, epoch_id):
        """
        - infers schema for `tbl`
        - writes Bronze (Delta) for this table
        - DOES NOT call dbt here (dbt is triggered after batch via debouncer)
        """
        schema = _infer_schema_from_batch(batch_df) or infer_schema_from_cdc_event(spark, tbl)
        if not schema:
            log.info(f'[DVH_CORE][STREAM][{tbl:}][epoch={epoch_id:}] no schema available; skipping')
            return None

        parsed_tbl = batch_df.select(from_json(col("payload"), schema).alias("r")).select("r.*").coalesce(8)

        batch_count = parsed_tbl.count()
        if batch_count == 0:
            log.debug(f'[DVH_CORE][STREAM][{tbl:}][epoch={epoch_id:}] empty micro-batch; skipping')
            return None

        # Write Bronze (keep your existing write path/options)
        table_path = os.path.join(STAGING_BASE_PATH, tbl)
        (parsed_tbl.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .save(table_path)
         )
        log.info(f'[DVH_CORE][BRONZE][{tbl:}][epoch={epoch_id:}] wrote {batch_count:} rows to {table_path:}')
        return tbl

    def _foreach_batch(batch_df, epoch_id: int):
        if batch_df.rdd.isEmpty():
            log.debug(f'[DVH_CORE][STREAM][epoch={epoch_id:}] empty micro-batch')
            return
        touched = [r["table"] for r in batch_df.select("table").distinct().collect() if r["table"]]
        if not touched:
            log.debug(f"[DVH_CORE][STREAM][epoch={epoch_id:}] no 'table' values present")
            return
        for tbl in touched:
            try:
                _process_table(batch_df.where(col("table") == tbl), tbl, epoch_id)
            except Exception as e:
                log.exception(f'[DVH_CORE][STREAM][{tbl:}][epoch={epoch_id:}] processing failed: {e:}')

    table_to_models = get_existing_model_tables()

    # Be tolerant to different callback signatures from bronze_ingestor
    def _after_write(*args, **kwargs):
        # Accept (touched,) or (touched, batch_id, counts)
        if not args:
            return
        written_tables = args[0] or []
        if not written_tables:
            return

        nonlocal table_to_models

        # Refresh mapping if we see tables we don't know yet (models are generated at runtime).
        if any(tbl not in table_to_models for tbl in written_tables):
            table_to_models = get_existing_model_tables()

        models = set()
        for tbl in written_tables:
            for m in table_to_models.get(tbl, []):
                models.add(m)

        if models:
            log.info(f'[DVH_CORE][DBT-QUEUE] epoch models={sorted(models):}')
            queue_dbt_models(models)

    # New datasets would be dropped, producing empty micro-batches and E2E timeouts.
    allowed_tables = None

    query = start_bronze_writer(
        spark=spark,
        df_stream=src,
        table_name=None,
        table_col="table",
        checkpoint_base=checkpoint_dir,
        on_after_write=_after_write,
        allowed_tables=allowed_tables,
        query_name=f"{KAFKA_TOPIC}-generic-ingestor",
        trigger_every=STREAM_TRIGGER,
    )

    log.info(f'[DVH_CORE][STREAM] Query started: id={query.id:}, name={query.name:}, trigger={STREAM_TRIGGER:}, checkpoint={checkpoint_dir:}')
    # threading.Thread(target=log_progress_periodically, args=(query,), daemon=True).start()
    return query

