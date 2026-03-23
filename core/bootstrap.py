"""Bootstrap for bronze tables.

Extracted from the original main.py without behavioral changes.
"""

from logger import log
from helper.spark_helper import get_spark_session
from utils.bronze_ingestor import ensure_bronze_table_exists
from utils.schema_helpers import bronze_target_columns
from utils.performance_logger import PerfListener

def bootstrap_bronze(lake_tables, load_table):
    from pyspark.sql.types import StructType, StructField, StringType
    spark = get_spark_session("DataVault_Bootstrap")
    spark.streams.addListener(PerfListener())

    for t in lake_tables:
        # Get column names from the lake; make a simple all-STRING schema for the empty Bronze
        df_cols = bronze_target_columns(spark, t)
        if not df_cols:
            continue
        schema = StructType([StructField(c, StringType(), True) for c in df_cols])
        try:
            ensure_bronze_table_exists(spark, t, schema)
        except Exception as e:
            log.warning(f'[DVH_CORE][Bootstrap] Could not precreate bronze.{t}: {e}')

