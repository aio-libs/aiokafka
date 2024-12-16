from typing import NamedTuple

from aiokafka.protocol.struct import Struct
from aiokafka.structs import TopicPartition

class ConsumerProtocolMemberMetadata(Struct):
    version: int
    subscription: list[str]
    user_data: bytes
    SCHEMA = ...

class ConsumerProtocolMemberAssignment(Struct):
    class Assignment(NamedTuple):
        topic: str
        partitions: list[int]

    version: int
    assignment: list[Assignment]
    user_data: bytes
    SCHEMA = ...
    def partitions(self) -> list[TopicPartition]: ...

class ConsumerProtocol:
    PROTOCOL_TYPE = ...
    ASSIGNMENT_STRATEGIES = ...
    METADATA = ConsumerProtocolMemberMetadata
    ASSIGNMENT = ConsumerProtocolMemberAssignment
