from dataclasses import dataclass
from typing import Generic, NamedTuple, Optional, Sequence, Tuple, TypeVar

from kafka.structs import (
    BrokerMetadata,
    OffsetAndMetadata,
    PartitionMetadata,
    TopicPartition,
)


__all__ = [
    "OffsetAndMetadata",
    "TopicPartition",
    "RecordMetadata",
    "ConsumerRecord",
    "BrokerMetadata",
    "PartitionMetadata",
]


class RecordMetadata(NamedTuple):
    topic: str
    partition: int
    topic_partition: TopicPartition
    offset: int
    timestamp: Optional[int]  # Timestamp in millis, None for older Brokers
    timestamp_type: int


KT = TypeVar("KT")
VT = TypeVar("VT")


@dataclass
class ConsumerRecord(Generic[KT, VT]):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: Optional[KT]
    value: Optional[VT]
    checksum: int
    serialized_key_size: int
    serialized_value_size: int
    headers: Sequence[Tuple[str, bytes]]


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: Optional[int]  # Only None if used with old broker version
