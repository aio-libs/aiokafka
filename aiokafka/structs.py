import collections
from kafka.common import (
    OffsetAndMetadata, TopicPartition, BrokerMetadata, PartitionMetadata
)

__all__ = [
    "OffsetAndMetadata", "TopicPartition", "RecordMetadata", "ConsumerRecord",
    "BrokerMetadata", "PartitionMetadata"
]

RecordMetadata = collections.namedtuple(
    'RecordMetadata', ['topic', 'partition', 'topic_partition', 'offset',
                       'timestamp', 'timestamp_type'])

ConsumerRecord = collections.namedtuple(
    "ConsumerRecord", ["topic", "partition", "offset", "timestamp",
                       "timestamp_type", "key", "value", "checksum",
                       "serialized_key_size", "serialized_value_size",
                       "headers"])

OffsetAndTimestamp = collections.namedtuple(
    "OffsetAndTimestamp", ["offset", "timestamp"])
