from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, NamedTuple, TypeVar

from aiokafka.errors import KafkaError

__all__ = [
    "OffsetAndMetadata",
    "TopicPartition",
    "RecordMetadata",
    "ConsumerRecord",
    "BrokerMetadata",
    "PartitionMetadata",
]

class TopicPartition(NamedTuple):
    """A topic and partition tuple"""

    topic: str
    partition: int

class BrokerMetadata(NamedTuple):
    """A Kafka broker metadata used by admin tools"""

    nodeId: int | str
    host: str
    port: int
    rack: str | None

class PartitionMetadata(NamedTuple):
    """A topic partition metadata describing the state in the MetadataResponse"""

    topic: str
    partition: int
    leader: int
    replicas: list[int]
    isr: list[int]
    error: KafkaError | None

class OffsetAndMetadata(NamedTuple):
    """The Kafka offset commit API

    The Kafka offset commit API allows users to provide additional metadata
    (in the form of a string) when an offset is committed. This can be useful
    (for example) to store information about which node made the commit,
    what time the commit was made, etc.
    """

    offset: int
    metadata: str

class RecordMetadata(NamedTuple):
    """Returned when a :class:`~.AIOKafkaProducer` sends a message"""

    topic: str
    partition: int
    topic_partition: TopicPartition
    offset: int
    timestamp: int | None
    timestamp_type: int
    log_start_offset: int | None

KT = TypeVar("KT", covariant=True)
VT = TypeVar("VT", covariant=True)

@dataclass
class ConsumerRecord(Generic[KT, VT]):
    topic: str
    partition: int
    offset: int
    timestamp: int
    timestamp_type: int
    key: KT | None
    value: VT | None
    checksum: int | None
    serialized_key_size: int
    serialized_value_size: int
    headers: Sequence[tuple[str, bytes]]

class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: int | None
