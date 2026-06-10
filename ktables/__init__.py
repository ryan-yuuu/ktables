"""ktables — materialize Kafka topics into in-memory dicts (GlobalKTable for asyncio).

See the kafka_table module docstring for the consistency contract and
design notes, and README.md for usage.
"""

from ktables.kafka_table import (
    DEFAULT_TOPIC_CONFIGS,
    KafkaTable,
    KafkaTableWriter,
    SupportsJsonModel,
    TableStatus,
    ViewStats,
    ensure_topic,
)

__all__ = [
    "DEFAULT_TOPIC_CONFIGS",
    "KafkaTable",
    "KafkaTableWriter",
    "SupportsJsonModel",
    "TableStatus",
    "ViewStats",
    "ensure_topic",
]
