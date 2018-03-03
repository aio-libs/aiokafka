from collections import namedtuple, defaultdict

from aiokafka.structs import TopicPartition


PidAndEpoch = namedtuple("PidAndEpoch", ["pid", "epoch"])
NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


class TransactionManager:

    def __init__(self):
        self._pid_and_epoch = PidAndEpoch(NO_PRODUCER_ID, NO_PRODUCER_EPOCH)
        self._sequence_numbers = defaultdict(lambda: 0)

    def set_pid_and_epoch(self, pid: int, epoch: int):
        self._pid_and_epoch = PidAndEpoch(pid, epoch)

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
