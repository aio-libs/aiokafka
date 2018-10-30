import asyncio
from copy import copy
from enum import Enum
from collections import namedtuple, defaultdict

from aiokafka.structs import TopicPartition
from aiokafka.util import create_future


PidAndEpoch = namedtuple("PidAndEpoch", ["pid", "epoch"])
NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


class SubscriptionType(Enum):

    NONE = 1
    AUTO_TOPICS = 2
    AUTO_PATTERN = 3
    USER_ASSIGNED = 4


class TransactionResult:

    ABORT = 0
    COMMIT = 1


class TransactionState(Enum):

    UNINITIALIZED = 1
    INITIALIZING = 2
    READY = 3
    IN_TRANSACTION = 4
    COMMITTING_TRANSACTION = 5
    ABORTING_TRANSACTION = 6
    ERROR = 7

    @classmethod
    def is_transition_valid(cls, source, target):
        if target == cls.INITIALIZING:
            return source == cls.UNINITIALIZED
        elif target == cls.READY:
            return source == cls.INITIALIZING or \
                source == cls.COMMITTING_TRANSACTION or \
                source == cls.ABORTING_TRANSACTION or \
                source == cls.UNINITIALIZED  # XXX REMOVE ME
        elif target == cls.IN_TRANSACTION:
            return source == cls.READY
        elif target == cls.COMMITTING_TRANSACTION:
            return source == cls.IN_TRANSACTION
        elif target == cls.ABORTING_TRANSACTION:
            return source == cls.IN_TRANSACTION or source == cls.ERROR
        # We can transition to ERROR unconditionally.
        # ERROR is never a valid starting state for any transition. So the
        # only option is to close the producer or do purely non transactional
        # requests.
        else:
            return True


class TransactionManager:

    def __init__(self, transactional_id, transaction_timeout_ms, *, loop):
        self.transactional_id = transactional_id
        self.transaction_timeout_ms = transaction_timeout_ms
        self.state = TransactionState.UNINITIALIZED
        self._exception = None

        self._pid_and_epoch = PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._pid_waiter = create_future(loop)
        self._sequence_numbers = defaultdict(lambda: 0)
        self._transaction_waiter = None

        self._txn_partitions = set()
        self._pending_txn_partitions = set()

        self._loop = loop

    # INDEMPOTANCE PART

    def set_pid_and_epoch(self, pid: int, epoch: int):
        self._pid_and_epoch = PidAndEpoch(pid, epoch)
        if self.transactional_id:
            self._transition_to(TransactionState.READY)
            self._pid_waiter.set_result(None)

    def has_pid(self):
        return self._pid_and_epoch.pid != NO_PRODUCER_ID

    @asyncio.coroutine
    def wait_for_pid(self):
        if self.has_pid():
            return
        else:
            yield from self._pid_waiter

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
        if self.state == TransactionState.ERROR:
            raise copy(self._exception)

        assert TransactionState.is_transition_valid(self.state, target), \
            "Invalid state transition {} -> {}".format(self.state, target)
        self.state = target

    def is_fenced(self):
        return self.state == TransactionState.FENCED

    def init_transactions(self):
        self._transition_to(TransactionState.INITIALIZING)

    def begin_transaction(self):
        self._transition_to(TransactionState.IN_TRANSACTION)
        self._transaction_waiter = create_future(loop=self._loop)

    def committing_transaction(self):
        self._transition_to(TransactionState.COMMITTING_TRANSACTION)

    def aborting_transaction(self):
        self._transition_to(TransactionState.ABORTING_TRANSACTION)

    def complete_transaction(self):
        assert not self._pending_txn_partitions
        self._transition_to(TransactionState.READY)
        self._txn_partitions.clear()
        self._transaction_waiter.set_result(None)

    def transition_to_error(self, exc):
        self._transition_to(TransactionState.ERROR)
        self._exception = exc

    def maybe_add_partition_to_txn(self, tp: TopicPartition):
        if self.transactional_id is None:
            return
        assert self.state == TransactionState.IN_TRANSACTION
        if tp not in self._txn_partitions:
            self._pending_txn_partitions.add(tp)

    def partitions_to_add(self):
        return self._pending_txn_partitions

    def partition_added(self, tp: TopicPartition):
        self._pending_txn_partitions.remove(tp)
        self._txn_partitions.add(tp)

    @property
    def txn_partitions(self):
        return self._txn_partitions

    def needs_transaction_commit(self):
        if self.state == TransactionState.COMMITTING_TRANSACTION:
            return TransactionResult.COMMIT
        elif self.state == TransactionState.ABORTING_TRANSACTION:
            return TransactionResult.ABORT
        else:
            return

    def needs_transaction_abort(self):
        return

    def wait_for_transaction_end(self):
        return self._transaction_waiter
