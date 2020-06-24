from collections import namedtuple
from typing import NamedTuple, Optional, Sequence


class TopicPartition(NamedTuple):
    topic: str
    partition: int


class BrokerMetadata(NamedTuple):
    nodeId: int
    host: str
    port: int
    rack: str


class PartitionMetadata(NamedTuple):
    topic: str
    partition: int
    leader: int
    replicas: Sequence[int]
    isr: Sequence[int]
    error: int


class OffsetAndMetadata(NamedTuple):
    offset: int
    metadata: str


class OffsetAndTimestamp(NamedTuple):
    offset: int
    timestamp: Optional[int]  # Only None if used with old broker version


# Deprecated part
RetryOptions = namedtuple('RetryOptions', ['limit', 'backoff_ms', 'retry_on_timeouts'])
