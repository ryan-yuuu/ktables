"""ktables — materialize Kafka topics into in-memory dicts (GlobalKTable for asyncio).

See the kafka_table module docstring for the consistency contract and
design notes, and README.md for usage. Grouped tables (a nested
``group → {member → value}`` view over a compacted topic) live in
``grouped_table`` and are re-exported here.
"""

from ktables.grouped_table import (
    DEFAULT_KEY_CODEC,
    CompositeKeyCodec,
    GroupedKafkaTable,
    GroupedKafkaTableWriter,
    LengthPrefixedKeyCodec,
)
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
    "CompositeKeyCodec",
    "DEFAULT_KEY_CODEC",
    "DEFAULT_TOPIC_CONFIGS",
    "GroupedKafkaTable",
    "GroupedKafkaTableWriter",
    "KafkaTable",
    "KafkaTableWriter",
    "LengthPrefixedKeyCodec",
    "SupportsJsonModel",
    "TableStatus",
    "ViewStats",
    "ensure_topic",
]
