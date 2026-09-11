import pytest

from aiokafka.partitioner import DefaultPartitioner, RoundRobinPartitioner, murmur2


def test_default_partitioner():
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(100))
    # partitioner should return the same partition for the same key
    p1 = partitioner(b"foo", all_partitions, available)
    p2 = partitioner(b"foo", all_partitions, available)
    assert p1 == p2
    assert p1 in all_partitions

    # when key is None, choose one of available partitions
    assert partitioner(None, all_partitions, [123]) == 123

    # with fallback to all_partitions
    assert partitioner(None, all_partitions, []) in all_partitions


@pytest.mark.parametrize(
    "bytes_payload,partition_number",
    [
        (b"", 681),
        (b"a", 524),
        (b"ab", 434),
        (b"abc", 107),
        (b"123456789", 566),
        (b"\x00 ", 742),
    ],
)
def test_murmur2_java_compatibility(bytes_payload, partition_number):
    partitioner = DefaultPartitioner()
    all_partitions = available = list(range(1000))
    # compare with output from Kafka's org.apache.kafka.clients.producer.Partitioner
    assert partitioner(bytes_payload, all_partitions, available) == partition_number


def test_murmur2_not_ascii():
    # Verify no regression of murmur2() bug encoding py2 bytes that don't ascii encode
    murmur2(b"\xa4")
    murmur2(b"\x81" * 1000)


def test_round_robin_partitioner():
    partitioner = RoundRobinPartitioner()

    all_partitions = available_partitions = list(range(2))
    assert partitioner(None, all_partitions, available_partitions) == 0
    assert partitioner(None, all_partitions, available_partitions) == 1
    assert partitioner(None, all_partitions, available_partitions) == 0
    assert partitioner(None, all_partitions, available_partitions) == 1

    all_partitions = available_partitions = list(range(4))
    assert partitioner(None, all_partitions, available_partitions) == 2
    assert partitioner(None, all_partitions, available_partitions) == 3

    all_partitions = [50]
    available_partitions = [70]
    assert partitioner(None, all_partitions, available_partitions) == 70
