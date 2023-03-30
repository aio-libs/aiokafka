from typing import NamedTuple
from kafka.errors import KafkaError

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
    replicas: list[int]
    isr: list[int]
    error: KafkaError

class OffsetAndMetadata(NamedTuple):
    offset: int
    metadata: str
