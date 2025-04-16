from typing import Any, TypeVar

__all__ = [
    "ConsumerStoppedError",
    "NoOffsetForPartitionError",
    "RecordTooLargeError",
    "ProducerClosed",
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
    "IncompatibleBrokerVersion",
    "CommitFailedError",
    "AuthenticationMethodNotSupported",
    "AuthenticationFailedError",
    "BrokerResponseError",
    "NoError",
    "UnknownError",
    "OffsetOutOfRangeError",
    "CorruptRecordException",
    "UnknownTopicOrPartitionError",
    "InvalidFetchRequestError",
    "LeaderNotAvailableError",
    "NotLeaderForPartitionError",
    "RequestTimedOutError",
    "BrokerNotAvailableError",
    "ReplicaNotAvailableError",
    "MessageSizeTooLargeError",
    "StaleControllerEpochError",
    "OffsetMetadataTooLargeError",
    "StaleLeaderEpochCodeError",
    "GroupLoadInProgressError",
    "GroupCoordinatorNotAvailableError",
    "NotCoordinatorForGroupError",
    "InvalidTopicError",
    "RecordListTooLargeError",
    "NotEnoughReplicasError",
    "NotEnoughReplicasAfterAppendError",
    "InvalidRequiredAcksError",
    "IllegalGenerationError",
    "InconsistentGroupProtocolError",
    "InvalidGroupIdError",
    "UnknownMemberIdError",
    "InvalidSessionTimeoutError",
    "RebalanceInProgressError",
    "InvalidCommitOffsetSizeError",
    "TopicAuthorizationFailedError",
    "GroupAuthorizationFailedError",
    "ClusterAuthorizationFailedError",
    "InvalidTimestampError",
    "UnsupportedSaslMechanismError",
    "IllegalSaslStateError",
    "UnsupportedVersionError",
    "TopicAlreadyExistsError",
    "InvalidPartitionsError",
    "InvalidReplicationFactorError",
    "InvalidReplicationAssignmentError",
    "InvalidConfigurationError",
    "NotControllerError",
    "InvalidRequestError",
    "UnsupportedForMessageFormatError",
    "PolicyViolationError",
    "KafkaUnavailableError",
    "KafkaTimeoutError",
    "KafkaConnectionError",
    "UnsupportedCodecError",
]

class KafkaError(RuntimeError):
    retriable = ...
    invalid_metadata = ...
    def __str__(self) -> str: ...

class IllegalStateError(KafkaError): ...
class IllegalArgumentError(KafkaError): ...

class NoBrokersAvailable(KafkaError):
    retriable = ...
    invalid_metadata = ...

class NodeNotReadyError(KafkaError):
    retriable = ...

class KafkaProtocolError(KafkaError):
    retriable = ...

class CorrelationIdError(KafkaProtocolError):
    retriable = ...

class Cancelled(KafkaError):
    retriable = ...

class TooManyInFlightRequests(KafkaError):
    retriable = ...

class StaleMetadata(KafkaError):
    retriable = ...
    invalid_metadata = ...

class MetadataEmptyBrokerList(KafkaError):
    retriable = ...

class UnrecognizedBrokerVersion(KafkaError): ...
class IncompatibleBrokerVersion(KafkaError): ...

class CommitFailedError(KafkaError):
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...

class AuthenticationMethodNotSupported(KafkaError): ...

class AuthenticationFailedError(KafkaError):
    retriable = ...

class KafkaUnavailableError(KafkaError): ...
class KafkaTimeoutError(KafkaError): ...

class KafkaConnectionError(KafkaError):
    retriable = ...
    invalid_metadata = ...

class UnsupportedCodecError(KafkaError): ...
class KafkaConfigurationError(KafkaError): ...
class QuotaViolationError(KafkaError): ...

class ConsumerStoppedError(Exception):
    """Raised on `get*` methods of Consumer if it's cancelled, even pending
    ones.
    """

class IllegalOperation(Exception):
    """Raised if you try to execute an operation, that is not available with
    current configuration. For example trying to commit if no group_id was
    given.
    """

class NoOffsetForPartitionError(KafkaError): ...
class RecordTooLargeError(KafkaError): ...
class ProducerClosed(KafkaError): ...

class ProducerFenced(KafkaError):
    """Another producer with the same transactional ID went online.
    NOTE: As it seems this will be raised by Broker if transaction timeout
    occurred also.
    """

    def __init__(self, msg: str = ...) -> None: ...

class BrokerResponseError(KafkaError):
    errno: int
    message: str
    description: str = ...
    def __str__(self) -> str:
        """Add errno to standard KafkaError str"""

class NoError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnknownError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class OffsetOutOfRangeError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class CorruptRecordException(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

InvalidMessageError = CorruptRecordException

class UnknownTopicOrPartitionError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...
    invalid_metadata = ...

class InvalidFetchRequestError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class LeaderNotAvailableError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...
    invalid_metadata = ...

class NotLeaderForPartitionError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...
    invalid_metadata = ...

class RequestTimedOutError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

class BrokerNotAvailableError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class ReplicaNotAvailableError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class MessageSizeTooLargeError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class StaleControllerEpochError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class OffsetMetadataTooLargeError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class StaleLeaderEpochCodeError(BrokerResponseError):
    errno = ...
    message = ...

class GroupLoadInProgressError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

CoordinatorLoadInProgressError = GroupLoadInProgressError

class GroupCoordinatorNotAvailableError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

CoordinatorNotAvailableError = GroupCoordinatorNotAvailableError

class NotCoordinatorForGroupError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

NotCoordinatorError = NotCoordinatorForGroupError

class InvalidTopicError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class RecordListTooLargeError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class NotEnoughReplicasError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

class NotEnoughReplicasAfterAppendError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

class InvalidRequiredAcksError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class IllegalGenerationError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InconsistentGroupProtocolError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidGroupIdError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnknownMemberIdError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidSessionTimeoutError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class RebalanceInProgressError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidCommitOffsetSizeError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class TopicAuthorizationFailedError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class GroupAuthorizationFailedError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class ClusterAuthorizationFailedError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidTimestampError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnsupportedSaslMechanismError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class IllegalSaslStateError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnsupportedVersionError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class TopicAlreadyExistsError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidPartitionsError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidReplicationFactorError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidReplicationAssignmentError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidConfigurationError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class NotControllerError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...
    retriable = ...

class InvalidRequestError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnsupportedForMessageFormatError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class PolicyViolationError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class OutOfOrderSequenceNumber(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DuplicateSequenceNumber(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidProducerEpoch(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidTxnState(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidProducerIdMapping(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidTransactionTimeout(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class ConcurrentTransactions(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class TransactionCoordinatorFenced(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class TransactionalIdAuthorizationFailed(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class SecurityDisabled(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class OperationNotAttempted(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class KafkaStorageError(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class LogDirNotFound(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class SaslAuthenticationFailed(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class UnknownProducerId(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class ReassignmentInProgress(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenAuthDisabled(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenNotFound(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenOwnerMismatch(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenRequestNotAllowed(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenAuthorizationFailed(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class DelegationTokenExpired(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidPrincipalType(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class NonEmptyGroup(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class GroupIdNotFound(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class FetchSessionIdNotFound(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class InvalidFetchSessionEpoch(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class ListenerNotFound(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

class MemberIdRequired(BrokerResponseError):
    errno = ...
    message = ...
    description = ...

_T = TypeVar("_T", bound=type)
kafka_errors = ...

def for_code(error_code: int) -> type[BrokerResponseError]: ...
