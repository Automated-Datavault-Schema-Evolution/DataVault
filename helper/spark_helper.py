"""Spark helper facade.

This module exists to keep Spark-specific utilities isolated from orchestration code.
Functionality is unchanged; functions are re-exported from utils.helper_spark.
"""

from utils.helper_spark import (
    get_spark_session,
    ensure_spark_warehouse_dir,
    get_active_stream_query_by_name,
)

__all__ = [
    "get_spark_session",
    "ensure_spark_warehouse_dir",
    "get_active_stream_query_by_name",
]
