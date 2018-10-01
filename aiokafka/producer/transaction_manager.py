from enum import Enum
from collections import namedtuple, defaultdict

from aiokafka.structs import TopicPartition


PidAndEpoch = namedtuple("PidAndEpoch", ["pid", "epoch"])
NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


class SubscriptionType(Enum):

    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class TransactionState(Enum):

    UNINITIALIZED = 1
    INITIALIZING = 2
    READY = 3
    IN_TRANSACTION = 4
    COMMITTING_TRANSACTION = 5
    ABORTING_TRANSACTION = 6
    FENCED = 7
    ERROR = 8

    @classmethod
    def is_transition_valid(cls, source, target):
        if target == cls.INITIALIZING:
            return source == cls.UNINITIALIZED or source == cls.ERROR
        elif target == cls.READY:
            return source == cls.INITIALIZING or \
                source == cls.COMMITTING_TRANSACTION or \
                source == cls.ABORTING_TRANSACTION
        elif target == cls.IN_TRANSACTION:
            return source == cls.READY
        elif target == cls.COMMITTING_TRANSACTION:
            return source == cls.IN_TRANSACTION
        elif target == cls.ABORTING_TRANSACTION:
            return source == cls.IN_TRANSACTION or source == cls.ERROR
        # We can transition to FENCED or ERROR unconditionally.
        # FENCED is never a valid starting state for any transition. So the
        # only option is to close the producer or do purely non transactional
        # requests.
        else:
            return True


class TransactionManager:

    def __init__(self, transactional_id, transaction_timeout_ms):
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.state = TransactionState.UNINITIALIZED

        self._pid_and_epoch = PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._sequence_numbers = defaultdict(lambda: 0)

    # INDEMPOTANCE PART

    def set_pid_and_epoch(self, pid: int, epoch: int):
        self._pid_and_epoch = PidAndEpoch(pid, epoch)
        if self.transactional_id:
            self._transition_to(TransactionState.READY)

    def has_pid(self):
        return self._pid_and_epoch.pid != NO_PRODUCER_ID

    def reset_producer_id(self):
        """ This method is used when the producer needs to reset it's internal
        state because of an irrecoverable exception from the broker.
            In all of these cases, we don't know whether batch was actually
        committed on the broker, and hence whether the sequence number was
        actually updated. If we don't reset the producer state, we risk the
        chance that all future messages will return an
        ``OutOfOrderSequenceException``.
        """
        self._pid_and_epoch = PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._sequence_numbers.clear()

    def sequence_number(self, tp: TopicPartition):
        return self._sequence_numbers[tp]

    def increment_sequence_number(self, tp: TopicPartition, increment: int):
        # Java will wrap those automatically, but in Python we will break
        # on `struct.pack` if ints are too big, so we do it here
        seq = self._sequence_numbers[tp] + increment
        if seq > 2 ** 31 - 1:
            seq -= 2 ** 32
        self._sequence_numbers[tp] = seq

    @property
    def producer_id(self):
        return self._pid_and_epoch.pid

    @property
    def producer_epoch(self):
        return self._pid_and_epoch.epoch

    # TRANSACTION PART

    def _transition_to(self, target):
        assert TransactionState.is_transition_valid(self.state, target), \
            "Invalid state transition {} -> {}".format(self.state, target)
        self.state = target

    def init_transactions(self):
        self._transition_to(TransactionState.INITIALIZING)

    def begin_transaction(self):
        self._transition_to(TransactionState.IN_TRANSACTION)

    def committing_transaction(self):
        self._transition_to(TransactionState.COMMITTING_TRANSACTION)

    def aborting_transaction(self):
        self._transition_to(TransactionState.ABORTING_TRANSACTION)

    def complete_transaction(self):
        self._transition_to(TransactionState.READY)
