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
    ProducerFenced, OutOfOrderSequenceNumber
    # ProducerClosed
)
from aiokafka.structs import TopicPartition


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('<0.11.0')
    @run_until_complete
    async def test_producer_transactions_not_supported(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        producer
        with self.assertRaises(UnsupportedVersionError):
            await producer.start()
        await producer.stop()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_simple(self):
        # The test here will just check if we can do simple produce with
        # transactional_id option and minimal setup.

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        async with producer.transaction():
            meta = await producer.send_and_wait(
                self.topic, b'hello, Kafka!')

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest")
        await consumer.start()
        self.add_cleanup(consumer.stop)
        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"hello, Kafka!")
        self.assertEqual(msg.key, None)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_fences_off_previous(self):
        # Test 2 producers fencing one another by using the same
        # transactional_id

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p2")
        await producer2.start()
        self.add_cleanup(producer2.stop)
        async with producer2.transaction():
            await producer2.send_and_wait(self.topic, b'hello, Kafka! 2')

        with self.assertRaises(ProducerFenced):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'hello, Kafka!')

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_restart_reaquire_pid(self):
        # While it's documented that PID may change we need to be sure we
        # are sending proper InitPIDRequest, not an indempotent one

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        pid = producer._txn_manager.producer_id
        await producer.stop()

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p2")
        await producer2.start()
        self.add_cleanup(producer2.stop)
        self.assertEqual(pid, producer2._txn_manager.producer_id)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_raise_out_of_sequence(self):
        # If we were to fail to send some message we should get\
        # OutOfOrderSequenceNumber

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(OutOfOrderSequenceNumber):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'msg1', partition=0)
                # Imitate a not delivered message
                producer._txn_manager.increment_sequence_number(
                    TopicPartition(self.topic, 0), 1)
                await producer.send_and_wait(self.topic, b'msg2', partition=0)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_aborting_previous_failure(self):
        # If we were to fail to send some message we should get\
        # OutOfOrderSequenceNumber

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(OutOfOrderSequenceNumber):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'msg1', partition=0)
                # Imitate a not delivered message
                producer._txn_manager.increment_sequence_number(
                    TopicPartition(self.topic, 0), 1)
                await producer.send_and_wait(self.topic, b'msg2', partition=0)
