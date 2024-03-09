from aiokafka.protocol.struct import Struct
from aiokafka.protocol.types import Array, Bytes, Int16, Int32, Schema, String
from aiokafka.structs import TopicPartition


class ConsumerProtocolMemberMetadata(Struct):
    SCHEMA = Schema(
        ("version", Int16),
        ("subscription", Array(String("utf-8"))),
        ("user_data", Bytes),
    )


class ConsumerProtocolMemberAssignment(Struct):
    SCHEMA = Schema(
        ("version", Int16),
        ("assignment", Array(("topic", String("utf-8")), ("partitions", Array(Int32)))),
        ("user_data", Bytes),
    )

    def partitions(self):
        return [
            TopicPartition(topic, partition)
            for topic, partitions in self.assignment
            for partition in partitions
        ]


class ConsumerProtocol:
    PROTOCOL_TYPE = "consumer"
    ASSIGNMENT_STRATEGIES = ("range", "roundrobin")
    METADATA = ConsumerProtocolMemberMetadata
    ASSIGNMENT = ConsumerProtocolMemberAssignment
