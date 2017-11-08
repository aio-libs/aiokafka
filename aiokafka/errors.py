from kafka.errors import *  # noqa
from kafka.errors import KafkaError, InvalidMessageError

__all__ = [
    # kafka-python errors
    "KafkaError", "ConnectionError", "NodeNotReadyError",
    "KafkaTimeoutError", "UnknownTopicOrPartitionError",
    "UnrecognizedBrokerVersion", "NotLeaderForPartitionError",
    "LeaderNotAvailableError", "TopicAuthorizationFailedError",
    "OffsetOutOfRangeError", "MessageSizeTooLargeError", "for_code", "NoError",
    "StaleMetadata", "CorrelationIdError", "NoBrokersAvailable",
    "RebalanceInProgressError", "IllegalGenerationError",
    "UnknownMemberIdError", "GroupLoadInProgressError",
    "GroupCoordinatorNotAvailableError", "NotCoordinatorForGroupError",
    "GroupAuthorizationFailedError", "IllegalStateError",
    "UnsupportedVersionError", "CorruptRecordException", "InvalidMessageError",
    # aiokafka custom errors
    "ConsumerStoppedError", "NoOffsetForPartitionError", "RecordTooLargeError",
    "ProducerClosed"
]

CorruptRecordException = InvalidMessageError


class ConsumerStoppedError(Exception):
    """ Raised on `get*` methods of Consumer if it's cancelled, even pending
        ones.
    """


class IllegalOperation(Exception):
    """ Raised if you try to execute an operation, that is not available with
        current configuration. For example trying to commit if no group_id was
        given.
    """


class NoOffsetForPartitionError(KafkaError):
    pass


class RecordTooLargeError(KafkaError):
    pass


class ProducerClosed(KafkaError):
    pass
