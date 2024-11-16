from typing import List, NamedTuple
from aiokafka.protocol.struct import Struct
from aiokafka.structs import TopicPartition

class ConsumerProtocolMemberMetadata(Struct):
    version: int
    subscription: List[str]
    user_data: bytes
    SCHEMA = ...

class ConsumerProtocolMemberAssignment(Struct):
    class Assignment(NamedTuple):
        topic: str
        partitions: List[int]
        ...

    version: int
    assignment: List[Assignment]
    user_data: bytes
    SCHEMA = ...
    def partitions(self) -> List[TopicPartition]: ...

class ConsumerProtocol:
    PROTOCOL_TYPE = ...
    ASSIGNMENT_STRATEGIES = ...
    METADATA = ConsumerProtocolMemberMetadata
    ASSIGNMENT = ConsumerProtocolMemberAssignment
