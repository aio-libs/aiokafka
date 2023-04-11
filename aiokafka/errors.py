import inspect
import sys

from kafka.errors import (
    KafkaError,
    IllegalStateError,
    IllegalArgumentError,
    NoBrokersAvailable,
    NodeNotReadyError,
    KafkaProtocolError,
    CorrelationIdError,
    Cancelled,
    TooManyInFlightRequests,
    StaleMetadata,
    UnrecognizedBrokerVersion,
    CommitFailedError,
    AuthenticationMethodNotSupported,
    AuthenticationFailedError,
    BrokerResponseError,

    # Numbered errors
    NoError,  # 0
    UnknownError,  # -1
    OffsetOutOfRangeError,  # 1
    CorruptRecordException,  # 2
    UnknownTopicOrPartitionError,  # 3
    InvalidFetchRequestError,  # 4
    LeaderNotAvailableError,  # 5
    NotLeaderForPartitionError,  # 6
    RequestTimedOutError,  # 7
    BrokerNotAvailableError,  # 8
    ReplicaNotAvailableError,  # 9
    MessageSizeTooLargeError,  # 10
    StaleControllerEpochError,  # 11
    OffsetMetadataTooLargeError,  # 12
    StaleLeaderEpochCodeError,  # 13
    GroupLoadInProgressError,  # 14
    GroupCoordinatorNotAvailableError,  # 15
    NotCoordinatorForGroupError,  # 16
    InvalidTopicError,  # 17
    RecordListTooLargeError,  # 18
    NotEnoughReplicasError,  # 19
    NotEnoughReplicasAfterAppendError,  # 20
    InvalidRequiredAcksError,  # 21
    IllegalGenerationError,  # 22
    InconsistentGroupProtocolError,  # 23
    InvalidGroupIdError,  # 24
    UnknownMemberIdError,  # 25
    InvalidSessionTimeoutError,  # 26
    RebalanceInProgressError,  # 27
    InvalidCommitOffsetSizeError,  # 28
    TopicAuthorizationFailedError,  # 29
    GroupAuthorizationFailedError,  # 30
    ClusterAuthorizationFailedError,  # 31
    InvalidTimestampError,  # 32
    UnsupportedSaslMechanismError,  # 33
    IllegalSaslStateError,  # 34
    UnsupportedVersionError,  # 35
    TopicAlreadyExistsError,  # 36
    InvalidPartitionsError,  # 37
    InvalidReplicationFactorError,  # 38
    InvalidReplicationAssignmentError,  # 39
    InvalidConfigurationError,  # 40
    NotControllerError,  # 41
    InvalidRequestError,  # 42
    UnsupportedForMessageFormatError,  # 43
    PolicyViolationError,  # 44

    KafkaUnavailableError,
    KafkaTimeoutError,
    KafkaConnectionError,
    UnsupportedCodecError,
)

__all__ = [
    # aiokafka custom errors
    "ConsumerStoppedError", "NoOffsetForPartitionError", "RecordTooLargeError",
    "ProducerClosed",

    # Kafka Python errors
    "KafkaError",
    "IllegalStateError",
    "IllegalArgumentError",
    "NoBrokersAvailable",
    "NodeNotReadyError",
    "KafkaProtocolError",
    "CorrelationIdError",
    "Cancelled",
    "TooManyInFlightRequests",
    "StaleMetadata",
    "UnrecognizedBrokerVersion",
    "CommitFailedError",
    "AuthenticationMethodNotSupported",
    "AuthenticationFailedError",
    "BrokerResponseError",

    # Numbered errors
    "NoError",  # 0
    "UnknownError",  # -1
    "OffsetOutOfRangeError",  # 1
    "CorruptRecordException",  # 2
    "UnknownTopicOrPartitionError",  # 3
    "InvalidFetchRequestError",  # 4
    "LeaderNotAvailableError",  # 5
    "NotLeaderForPartitionError",  # 6
    "RequestTimedOutError",  # 7
    "BrokerNotAvailableError",  # 8
    "ReplicaNotAvailableError",  # 9
    "MessageSizeTooLargeError",  # 10
    "StaleControllerEpochError",  # 11
    "OffsetMetadataTooLargeError",  # 12
    "StaleLeaderEpochCodeError",  # 13
    "GroupLoadInProgressError",  # 14
    "GroupCoordinatorNotAvailableError",  # 15
    "NotCoordinatorForGroupError",  # 16
    "InvalidTopicError",  # 17
    "RecordListTooLargeError",  # 18
    "NotEnoughReplicasError",  # 19
    "NotEnoughReplicasAfterAppendError",  # 20
    "InvalidRequiredAcksError",  # 21
    "IllegalGenerationError",  # 22
    "InconsistentGroupProtocolError",  # 23
    "InvalidGroupIdError",  # 24
    "UnknownMemberIdError",  # 25
    "InvalidSessionTimeoutError",  # 26
    "RebalanceInProgressError",  # 27
    "InvalidCommitOffsetSizeError",  # 28
    "TopicAuthorizationFailedError",  # 29
    "GroupAuthorizationFailedError",  # 30
    "ClusterAuthorizationFailedError",  # 31
    "InvalidTimestampError",  # 32
    "UnsupportedSaslMechanismError",  # 33
    "IllegalSaslStateError",  # 34
    "UnsupportedVersionError",  # 35
    "TopicAlreadyExistsError",  # 36
    "InvalidPartitionsError",  # 37
    "InvalidReplicationFactorError",  # 38
    "InvalidReplicationAssignmentError",  # 39
    "InvalidConfigurationError",  # 40
    "NotControllerError",  # 41
    "InvalidRequestError",  # 42
    "UnsupportedForMessageFormatError",  # 43
    "PolicyViolationError",  # 44

    "KafkaUnavailableError",
    "KafkaTimeoutError",
    "KafkaConnectionError",
    "UnsupportedCodecError",
]


class CoordinatorNotAvailableError(GroupCoordinatorNotAvailableError):
    message = "COORDINATOR_NOT_AVAILABLE"


class NotCoordinatorError(NotCoordinatorForGroupError):
    message = "NOT_COORDINATOR"


class CoordinatorLoadInProgressError(GroupLoadInProgressError):
    message = "COORDINATOR_LOAD_IN_PROGRESS"


InvalidMessageError = CorruptRecordException
GroupCoordinatorNotAvailableError = CoordinatorNotAvailableError
NotCoordinatorForGroupError = NotCoordinatorError
GroupLoadInProgressError = CoordinatorLoadInProgressError


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


class ProducerFenced(KafkaError):
    """Another producer with the same transactional ID went online.
       NOTE: As it seems this will be raised by Broker if transaction timeout
       occurred also.
    """

    def __init__(
        self,
        msg="There is a newer producer using the same transactional_id or"
            "transaction timeout occurred (check that processing time is "
            "below transaction_timeout_ms)"
    ):
        super().__init__(msg)


class OutOfOrderSequenceNumber(BrokerResponseError):
    errno = 45
    message = 'OUT_OF_ORDER_SEQUENCE_NUMBER'
    description = 'The broker received an out of order sequence number'


class DuplicateSequenceNumber(BrokerResponseError):
    errno = 46
    message = 'DUPLICATE_SEQUENCE_NUMBER'
    description = 'The broker received a duplicate sequence number'


class InvalidProducerEpoch(BrokerResponseError):
    errno = 47
    message = 'INVALID_PRODUCER_EPOCH'
    description = (
        'Producer attempted an operation with an old epoch. Either '
        'there is a newer producer with the same transactionalId, or the '
        'producer\'s transaction has been expired by the broker.'
    )


class InvalidTxnState(BrokerResponseError):
    errno = 48
    message = 'INVALID_TXN_STATE'
    description = (
        'The producer attempted a transactional operation in an invalid state'
    )


class InvalidProducerIdMapping(BrokerResponseError):
    errno = 49
    message = 'INVALID_PRODUCER_ID_MAPPING'
    description = (
        'The producer attempted to use a producer id which is not currently '
        'assigned to its transactional id'
    )


class InvalidTransactionTimeout(BrokerResponseError):
    errno = 50
    message = 'INVALID_TRANSACTION_TIMEOUT'
    description = (
        'The transaction timeout is larger than the maximum value allowed by'
        ' the broker (as configured by transaction.max.timeout.ms).'
    )


class ConcurrentTransactions(BrokerResponseError):
    errno = 51
    message = 'CONCURRENT_TRANSACTIONS'
    description = (
        'The producer attempted to update a transaction while another '
        'concurrent operation on the same transaction was ongoing'
    )


class TransactionCoordinatorFenced(BrokerResponseError):
    errno = 52
    message = 'TRANSACTION_COORDINATOR_FENCED'
    description = (
        'Indicates that the transaction coordinator sending a WriteTxnMarker'
        ' is no longer the current coordinator for a given producer'
    )


class TransactionalIdAuthorizationFailed(BrokerResponseError):
    errno = 53
    message = 'TRANSACTIONAL_ID_AUTHORIZATION_FAILED'
    description = 'Transactional Id authorization failed'


class SecurityDisabled(BrokerResponseError):
    errno = 54
    message = 'SECURITY_DISABLED'
    description = 'Security features are disabled'


class OperationNotAttempted(BrokerResponseError):
    errno = 55
    message = 'OPERATION_NOT_ATTEMPTED'
    description = (
        'The broker did not attempt to execute this operation. This may happen'
        ' for batched RPCs where some operations in the batch failed, causing '
        'the broker to respond without trying the rest.'
    )


class KafkaStorageError(BrokerResponseError):
    errno = 56
    message = 'KAFKA_STORAGE_ERROR'
    description = (
        'The user-specified log directory is not found in the broker config.'
    )


class LogDirNotFound(BrokerResponseError):
    errno = 57
    message = 'LOG_DIR_NOT_FOUND'
    description = (
        'The user-specified log directory is not found in the broker config.'
    )


class SaslAuthenticationFailed(BrokerResponseError):
    errno = 58
    message = 'SASL_AUTHENTICATION_FAILED'
    description = 'SASL Authentication failed.'


class UnknownProducerId(BrokerResponseError):
    errno = 59
    message = 'UNKNOWN_PRODUCER_ID'
    description = (
        'This exception is raised by the broker if it could not locate the '
        'producer metadata associated with the producerId in question. This '
        'could happen if, for instance, the producer\'s records were deleted '
        'because their retention time had elapsed. Once the last records of '
        'the producerId are removed, the producer\'s metadata is removed from'
        ' the broker, and future appends by the producer will return this '
        'exception.'
    )


class ReassignmentInProgress(BrokerResponseError):
    errno = 60
    message = 'REASSIGNMENT_IN_PROGRESS'
    description = 'A partition reassignment is in progress'


class DelegationTokenAuthDisabled(BrokerResponseError):
    errno = 61
    message = 'DELEGATION_TOKEN_AUTH_DISABLED'
    description = 'Delegation Token feature is not enabled'


class DelegationTokenNotFound(BrokerResponseError):
    errno = 62
    message = 'DELEGATION_TOKEN_NOT_FOUND'
    description = 'Delegation Token is not found on server.'


class DelegationTokenOwnerMismatch(BrokerResponseError):
    errno = 63
    message = 'DELEGATION_TOKEN_OWNER_MISMATCH'
    description = 'Specified Principal is not valid Owner/Renewer.'


class DelegationTokenRequestNotAllowed(BrokerResponseError):
    errno = 64
    message = 'DELEGATION_TOKEN_REQUEST_NOT_ALLOWED'
    description = (
        'Delegation Token requests are not allowed on PLAINTEXT/1-way SSL '
        'channels and on delegation token authenticated channels.'
    )


class DelegationTokenAuthorizationFailed(BrokerResponseError):
    errno = 65
    message = 'DELEGATION_TOKEN_AUTHORIZATION_FAILED'
    description = 'Delegation Token authorization failed.'


class DelegationTokenExpired(BrokerResponseError):
    errno = 66
    message = 'DELEGATION_TOKEN_EXPIRED'
    description = 'Delegation Token is expired.'


class InvalidPrincipalType(BrokerResponseError):
    errno = 67
    message = 'INVALID_PRINCIPAL_TYPE'
    description = 'Supplied principalType is not supported'


class NonEmptyGroup(BrokerResponseError):
    errno = 68
    message = 'NON_EMPTY_GROUP'
    description = 'The group is not empty'


class GroupIdNotFound(BrokerResponseError):
    errno = 69
    message = 'GROUP_ID_NOT_FOUND'
    description = 'The group id does not exist'


class FetchSessionIdNotFound(BrokerResponseError):
    errno = 70
    message = 'FETCH_SESSION_ID_NOT_FOUND'
    description = 'The fetch session ID was not found'


class InvalidFetchSessionEpoch(BrokerResponseError):
    errno = 71
    message = 'INVALID_FETCH_SESSION_EPOCH'
    description = 'The fetch session epoch is invalid'


class ListenerNotFound(BrokerResponseError):
    errno = 72
    message = 'LISTENER_NOT_FOUND'
    description = (
        'There is no listener on the leader broker that matches the'
        ' listener on which metadata request was processed'
    )


def _iter_broker_errors():
    for name, obj in inspect.getmembers(sys.modules[__name__]):
        if inspect.isclass(obj) and issubclass(obj, BrokerResponseError) and \
                obj != BrokerResponseError:
            yield obj


kafka_errors = {x.errno: x for x in _iter_broker_errors()}


def for_code(error_code):
    return kafka_errors.get(error_code, UnknownError)
