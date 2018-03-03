import pytest

from aiokafka.producer.transaction_manager import TransactionManager
from aiokafka.structs import TopicPartition

NO_PRODUCER_ID = -1
NO_PRODUCER_EPOCH = -1


@pytest.fixture
def txn_manager():
    return TransactionManager()


def test_txn_manager(txn_manager):
    assert txn_manager._pid_and_epoch.pid == NO_PRODUCER_ID
    assert txn_manager._pid_and_epoch.epoch == NO_PRODUCER_EPOCH
    assert not txn_manager._sequence_numbers
    assert not txn_manager.has_pid()

    txn_manager.set_pid_and_epoch(123, 321)
    assert txn_manager._pid_and_epoch.pid == 123
    assert txn_manager._pid_and_epoch.epoch == 321
    assert not txn_manager._sequence_numbers
    assert txn_manager.has_pid()

    tp1 = TopicPartition("topic", 1)
    tp2 = TopicPartition("topic", 2)

    assert txn_manager.sequence_number(tp1) == 0
    txn_manager.increment_sequence_number(tp1, 1)
    assert txn_manager.sequence_number(tp1) == 1

    # Changing one sequence_number does not change other
    assert txn_manager.sequence_number(tp2) == 0
    txn_manager.increment_sequence_number(tp2, 33)
    assert txn_manager.sequence_number(tp2) == 33
    assert txn_manager.sequence_number(tp1) == 1

    # sequence number should wrap around 32 bit signed integers
    txn_manager.increment_sequence_number(tp1, 2 ** 32 - 5)
    assert txn_manager.sequence_number(tp1) == -4

    txn_manager.reset_producer_id()
    assert txn_manager._pid_and_epoch.pid == NO_PRODUCER_ID
    assert txn_manager._pid_and_epoch.epoch == NO_PRODUCER_EPOCH
    assert not txn_manager._sequence_numbers
    assert not txn_manager.has_pid()
    assert txn_manager.sequence_number(tp1) == 0
    assert txn_manager.sequence_number(tp2) == 0
