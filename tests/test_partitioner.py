import pytest

from aiokafka.partitioner import DefaultPartitioner, murmur2
from aiokafka.producer import AIOKafkaProducer


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


class _FakeMetadata:
    def __init__(self, order):
        self._order = order

    def partitions_for_topic(self, topic):
        return list(self._order)

    def available_partitions_for_topic(self, topic):
        return list(self._order)


class _FakeProducer:
    # _partition() only touches these two attributes, so we can drive the real
    # method without spinning up a broker.
    def __init__(self, order):
        self._metadata = _FakeMetadata(order)
        self._partitioner = DefaultPartitioner()


def test_partition_selection_is_order_independent():
    # partitions_for_topic() returns a set, so the list handed to the
    # partitioner can come out in any iteration order (see issue #1127). A given
    # key must still map to the same partition regardless of that order.
    key = b"user-42"
    front = [7, 3, 40, 1, 22, 0, 15]
    orders = [
        list(range(50)),
        list(reversed(range(50))),
        front + [p for p in range(50) if p not in front],
    ]
    chosen = set()
    for order in orders:
        producer = _FakeProducer(order)
        chosen.add(
            AIOKafkaProducer._partition(producer, "t", None, key, None, key, None)
        )
    assert len(chosen) == 1
