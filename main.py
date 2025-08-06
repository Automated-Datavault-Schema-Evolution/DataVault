import os
import sys
import json
import time
import threading
import pandas as pd
from cdc_kafka_producer import cdc_producer_insert_only
from config import (
    LAKE_TYPE, PARQUET_PATH,
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, DBT_PROFILES_DIR, RDBMS_HOST, RDBMS_PORT, RDBMS_DB, RDBMS_USER,
    RDBMS_PASSWORD, RDBMS_SCHEMA,
)
from dv_modeller import extract_metadata, get_model_type, get_builder_code, split_datavault
from meta_store import write_lineage
from utils import get_spark_session
from logger import log

DBT_MODELS_PYTHON_DIR = os.path.join(os.path.dirname(__file__), "models", "python")
WATERMARK_FILE = "cdc_watermarks.json"

def indent_code(code, num_spaces=4):
    pad = ' ' * num_spaces
    return "\n".join(pad+line if line.strip() else "" for line in code.split("\n"))

def write_python_model_file(model_name, builder_code, table_name, model_type, meta):
    os.makedirs(DBT_MODELS_PYTHON_DIR, exist_ok=True)
    file_path = os.path.join(DBT_MODELS_PYTHON_DIR, f"{model_name}.py")
    with open("model_template.py.tpl", "r") as tpl:
        template = tpl.read()

    # builder_code = indent_code(builder_code)
    code = template.format(
        model_type=model_type,
        table_name=table_name,
        builder_code=builder_code,
        model_name=model_name,
        business_keys=json.dumps(list(meta['business_keys'])),
    )
    with open(file_path, "w") as file:
        file.write(code)
    log.info(f"[GEN] Generated DBT model for {model_name} (from lake table {table_name})")

def generate_schema_yml(table_names, output_path="models/schema.yml"):
    lines = []
    lines.append("version: 2")
    lines.append("")
    lines.append("sources:")
    lines.append("  - name: staging")
    lines.append("    tables:")
    for t in table_names:
        lines.append(f"      - name: {t}")
    with open(output_path, "w") as f:
        f.write("\n".join(lines))

def ensure_dbt_models_for_lake():
    log.debug(f"LAKE_TYPE: {LAKE_TYPE}")
    # Discover all source tables
    if LAKE_TYPE == "parquet":
        lake_tables = [f[:-8] for f in os.listdir(PARQUET_PATH) if f.endswith(".parquet")]
        load_table = lambda t: pd.read_parquet(os.path.join(PARQUET_PATH, t + ".parquet"), nrows=1)
    elif LAKE_TYPE == "rdbms":
        import psycopg2
        from psycopg2 import sql
        conn = psycopg2.connect(
            host=RDBMS_HOST, port=RDBMS_PORT,
            dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
        )
        log.debug(f"Connected to {RDBMS_HOST}:{RDBMS_PORT}")
        cur = conn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s AND table_type = %s",
            (RDBMS_SCHEMA, "BASE TABLE")
        )
        lake_tables = [row[0] for row in cur.fetchall()]
        log.debug(f"lake tables: {lake_tables}")
        cur.close()
        conn.close()
        def load_table(t):
            conn = psycopg2.connect(
                host=RDBMS_HOST, port=RDBMS_PORT,
                dbname=RDBMS_DB, user=RDBMS_USER, password=RDBMS_PASSWORD
            )
            cur = conn.cursor()
            q = sql.SQL("SELECT * FROM {}.{} LIMIT 1").format(sql.Identifier(RDBMS_SCHEMA), sql.Identifier(t))
            cur.execute(q)
            colnames = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()
            return pd.DataFrame(columns=colnames)
    else:
        raise ValueError("Unknown LAKE_TYPE")

    generate_schema_yml(lake_tables)

    # Generate models for only missing DV tables
    DBT_MODELS_PYTHON_DIR = os.path.join(os.path.dirname(__file__), "models", "python")
    DELTA_PATH = os.getenv("DELTA_PATH", "delta_vault")
    from utils import get_spark_session
    spark = get_spark_session("DV-Model-Check")

    from dv_modeller import extract_metadata, get_model_type, get_builder_code

    new_models = []
    for table in lake_tables:
        df_schema = load_table(table)
        meta = extract_metadata(df_schema)
        hubs, links, sats = split_datavault(table, meta['columns'])

        # Generate Hubs
        for hub in hubs:
            model_name = hub["name"]
            bk = hub["key"]
            builder_code = f'''out_df = df.select("{bk[0]}").distinct().withColumn("load_datetime", F.current_timestamp()).withColumn("{model_name}_hashkey", F.md5(F.concat_ws("||", df["{bk[0]}"].cast("string"))))'''
            write_python_model_file(
                model_name, builder_code, table, "hub", {'business_keys': bk, 'attributes': [], 'columns': [bk[0]]}
            )
            new_models.append(model_name)

        # Generate Links
        for link in links:
            model_name = link["name"]
            keys = link["keys"]
            key_select = ", ".join([f'"{k}"' for k in keys])
            hash_expr = ", ".join([f'df["{k}"].cast("string")' for k in keys])
            builder_code = f'''out_df = df.select({key_select}).distinct().withColumn("load_datetime", F.current_timestamp()).withColumn("{model_name}_hashkey", F.md5(F.concat_ws("||", {hash_expr})))'''
            write_python_model_file(
                model_name, builder_code, table, "link", {'business_keys': keys, 'attributes': [], 'columns': keys}
            )
            new_models.append(model_name)

        # Generate Satellites
        for sat in sats:
            model_name = sat["name"]
            keys = sat["key"]
            atts = sat["attributes"]
            select_cols = ", ".join([f'"{k}"' for k in keys + atts])
            hashdiff_expr = ", ".join([f'df["{c}"].cast("string")' for c in atts])
            builder_code = (
                f'out_df = df.select({select_cols})'
                f'.withColumn("load_datetime", F.current_timestamp())'
                f'.withColumn("{model_name}_hashdiff", F.md5(F.concat_ws("||", {hashdiff_expr})))'
            )
            write_python_model_file(
                model_name, builder_code, table, "sat",
                {'business_keys': keys, 'attributes': atts, 'columns': keys + atts}
            )
            new_models.append(model_name)
    return new_models

def ensure_profiles_dir():
    """
    Ensure dbt profiles directory exists in the project and create a default profiles.yml if needed.
    """
    if not os.path.exists(DBT_PROFILES_DIR):
        os.makedirs(DBT_PROFILES_DIR, exist_ok=True)
        log.info(f"[INFO] Created dbt profiles directory: {DBT_PROFILES_DIR}")

    # (Optional) Create a default profiles.yml if not present
    profiles_yml_path = os.path.join(DBT_PROFILES_DIR, "profiles.yml")
    if not os.path.exists(profiles_yml_path):
        with open(profiles_yml_path, "w") as f:
            f.write("# Insert your dbt profile config here\n")
        log.info(f"[INFO] Created empty profiles.yml at: {profiles_yml_path}")


# Spark consumer (Streaming + model generation)
def get_kafka_stream(spark, table_name, schema):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    df_data = df_table.select(from_json(col("json.payload"), schema).alias("data")).select("data.*")
    return df_data

def infer_schema_from_cdc_event(spark, table_name):
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    df_table = df_json.filter(col("json.table") == table_name)
    sample = df_table.limit(1).collect()
    if not sample:
        return None
    payload_json = json.loads(sample[0]["json"]["payload"])
    fields = [StructField(k, StringType(), True) for k in payload_json.keys()]
    return StructType(fields)


def streaming_dv_consumer_and_dbt():
    spark = get_spark_session("DataVault_Streaming_Consumer")
    from pyspark.sql.functions import col, from_json
    from pyspark.sql.types import StructType, StructField, StringType
    json_schema = StructType([
        StructField("table", StringType()),
        StructField("payload", StringType()),
        StructField("cdc_type", StringType()),
        StructField("modified_at", StringType())
    ])
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
    )
    df_json = df.select(from_json(col("value").cast("string"), json_schema).alias("json"))
    table_rows = df_json.groupBy(col("json.table")).count().collect()
    table_names = [row["table"] for row in table_rows if row["table"] is not None]

    for table_name in table_names:
        schema = infer_schema_from_cdc_event(spark, table_name)
        if not schema:
            log.info(f"[DBT Model] Skipping {table_name}: could not infer schema")
            continue
        df_stream = get_kafka_stream(spark, table_name, schema)
        meta = extract_metadata(df_stream)
        model_type = get_model_type(meta)
        model_name = f"{model_type}_{table_name}"
        builder_code = get_builder_code(table_name, model_type, meta)
        write_python_model_file(model_name, builder_code, table_name, model_type, meta)
        log.info(f"[DBT Model] Generated: {model_name}.py")

    log.info("[DBT] Running all models...")
    ensure_profiles_dir()
    exit_code = os.system(f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select python/*")
    if exit_code != 0:
        raise RuntimeError(f"dbt run failed with exit code {exit_code}")




def main():
    if os.path.exists(DBT_MODELS_PYTHON_DIR):
        for f in os.listdir(DBT_MODELS_PYTHON_DIR):
            os.remove(os.path.join(DBT_MODELS_PYTHON_DIR, f))
    else:
        os.makedirs(DBT_MODELS_PYTHON_DIR, exist_ok=True)

    new_models = ensure_dbt_models_for_lake()
    if new_models:
        log.info(f"[DBT] Running dbt for: {new_models}")
        ensure_profiles_dir()
        for model in new_models:
            os.system(f"dbt run --profiles-dir {DBT_PROFILES_DIR} --select {model}.py")
    else:
        log.info("NO new Data Vault tables to create, all up to date-")
    stop_event = threading.Event()
    cdc_thread = threading.Thread(target=cdc_producer_insert_only, daemon=True)
    cdc_thread.start()

    try:
        streaming_dv_consumer_and_dbt()
    except KeyboardInterrupt:
        log.info("Shutting down CDC producer...")
        stop_event.set()
        cdc_thread.join()
        log.info("All done.")

if __name__ == "__main__":
    main()
