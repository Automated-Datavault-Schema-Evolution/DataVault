"""Kafka helper facade.

This module exists to keep Kafka-specific utilities isolated from orchestration code.
It re-exports the producer and topic-management helpers used by the app.
"""

from cdc_kafka_producer import (
    cdc_producer_insert_only,
    check_and_create_topic,
    produce_tables_once,
)
from utils.helper_service_ready import wait_for_kafka


def kafka_topic_ready(*args, **kwargs):
    """Backward-compatible readiness probe."""
    try:
        wait_for_kafka(*args, **kwargs)
        return True
    except Exception:
        return False


def wait_kafka_topic_ready(*args, **kwargs):
    """Backward-compatible blocking readiness helper."""
    return wait_for_kafka(*args, **kwargs)


__all__ = [
    "cdc_producer_insert_only",
    "check_and_create_topic",
    "produce_tables_once",
    "kafka_topic_ready",
    "wait_kafka_topic_ready",
]
