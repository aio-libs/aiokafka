# import asyncio
# import gc
# import json
# import pytest
# import time
# import weakref
# from unittest import mock

# from kafka.cluster import ClusterMetadata

from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

# from aiokafka.protocol.produce import ProduceResponse
from aiokafka.producer import AIOKafkaProducer
# from aiokafka.client import AIOKafkaClient
from aiokafka.consumer import AIOKafkaConsumer
# from aiokafka.util import PY_341, create_future

from aiokafka.errors import (
    # KafkaTimeoutError, UnknownTopicOrPartitionError,
    # MessageSizeTooLargeError, NotLeaderForPartitionError,
    # LeaderNotAvailableError, RequestTimedOutError,
    UnsupportedVersionError,
    ProducerFenced
    # ProducerClosed
)


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('<0.11.0')
    @run_until_complete
    def test_producer_transactions_not_supported(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        producer
        with self.assertRaises(UnsupportedVersionError):
            yield from producer.start()
        yield from producer.stop()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    def test_producer_transactional_simple(self):
        # The test here will just check if we can do simple produce with
        # transactional_id option, as no specific API changes is expected.

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        yield from producer.start()
        self.add_cleanup(producer.stop)

        meta = yield from producer.send_and_wait(self.topic, b'hello, Kafka!')

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest")
        yield from consumer.start()
        self.add_cleanup(consumer.stop)
        msg = yield from consumer.getone()
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"hello, Kafka!")
        self.assertEqual(msg.key, None)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    def test_producer_transactional_fences_off_previous(self):
        # Test 2 producers fencing one another by using the same
        # transactional_id

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        yield from producer.start()
        self.add_cleanup(producer.stop)

        yield from producer.send_and_wait(self.topic, b'hello, Kafka!')

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        yield from producer2.start()
        self.add_cleanup(producer2.stop)

        with self.assertRaises(ProducerFenced):
            yield from producer.send_and_wait(self.topic, b'hello, Kafka! 2')
