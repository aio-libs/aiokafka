import asyncio
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
    # ProducerFenced, OutOfOrderSequenceNumber
    # ProducerClosed
)
# from aiokafka.structs import TopicPartition


class TestKafkaConsumerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('<0.11.0')
    @run_until_complete
    async def test_consumer_transactions_not_supported(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            isolation_level="read_committed")
        with self.assertRaises(UnsupportedVersionError):
            await consumer.start()
        await consumer.stop()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_transactional_commit(self):
        # The test here will just check if we can do simple produce with
        # transactional_id option and minimal setup.

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        await producer2.start()
        self.add_cleanup(producer2.stop)

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
            isolation_level="read_committed")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        # We will produce from a transactional producer and then from a
        # non-transactional. This should block consumption on that partition
        # until transaction is committed.
        await producer.begin_transaction()
        meta = await producer.send_and_wait(
            self.topic, b'Hello from transaction')

        meta2 = await producer2.send_and_wait(
            self.topic, b'Hello from non-transaction')

        # The transaction blocked consumption
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(1, loop=self.loop)
        self.assertFalse(task.done())

        await producer.commit_transaction()

        # Order should be preserved. We first yield the first message, although
        # it belongs to a committed afterwards transaction
        msg = await task
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"Hello from transaction")
        self.assertEqual(msg.key, None)

        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta2.offset)
        self.assertEqual(msg.timestamp, meta2.timestamp)
        self.assertEqual(msg.value, b"Hello from non-transaction")
        self.assertEqual(msg.key, None)
