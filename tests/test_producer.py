import json
import asyncio
from unittest import mock

from kafka.cluster import ClusterMetadata
from kafka.common import (KafkaTimeoutError,
                          UnknownTopicOrPartitionError,
                          MessageSizeTooLargeError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError,
                          RequestTimedOutError)
from kafka.protocol.produce import ProduceResponse

from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka.producer import AIOKafkaProducer
from aiokafka.message_accumulator import ProducerClosed


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = 'test_produce_topic'

    @run_until_complete
    def test_producer_start(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(loop=self.loop, acks=122)

        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(loop=self.loop, api_version="3.4.5")

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        self.assertNotEqual(producer._api_version, 'auto')
        with self.assertRaises(UnknownTopicOrPartitionError):
            yield from producer.partitions_for('some_topic_name')
        yield from self.wait_topic(producer.client, 'some_topic_name')
        partitions = yield from producer.partitions_for('some_topic_name')
        self.assertEqual(len(partitions), 2)
        self.assertEqual(partitions, set([0, 1]))
        yield from producer.stop()
        self.assertEqual(producer._closed, True)

    @run_until_complete
    def test_producer_send(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        with self.assertRaisesRegexp(AssertionError, 'value must be bytes'):
            yield from producer.send(self.topic, 'hello, Kafka!')
        future = yield from producer.send(self.topic, b'hello, Kafka!')
        resp = yield from future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        self.assertEqual(resp.offset, 0)

        fut = yield from producer.send(self.topic, b'second msg', partition=1)
        resp = yield from fut
        self.assertEqual(resp.partition, 1)

        future = yield from producer.send(self.topic, b'value', key=b'KEY')
        resp = yield from future
        self.assertTrue(resp.partition in (0, 1))
        yield from producer.stop()

        with self.assertRaises(ProducerClosed):
            yield from producer.send(self.topic, b'value', key=b'KEY')

    @run_until_complete
    def test_producer_send_noack(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts, acks=0)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        fut1 = yield from producer.send(
            self.topic, b'hello, Kafka!', partition=0)
        fut2 = yield from producer.send(
            self.topic, b'hello, Kafka!', partition=1)
        done, _ = yield from asyncio.wait([fut1, fut2], loop=self.loop)
        for item in done:
            self.assertEqual(item.result(), None)

    @run_until_complete
    def test_producer_send_with_serializer(self):
        def key_serializer(val):
            return val.upper().encode()

        def serializer(val):
            return json.dumps(val).encode()

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            value_serializer=serializer,
            key_serializer=key_serializer, acks='all',
            max_request_size=1000)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)
        key = 'some key'
        value = {'strKey': 23523.443, 23: 'STRval'}
        future = yield from producer.send(self.topic, value, key=key)
        resp = yield from future
        partition = resp.partition
        offset = resp.offset
        self.assertTrue(partition in (0, 1))  # partition

        future = yield from producer.send(self.topic, 'some str', key=key)
        resp = yield from future
        # expect the same partition bcs the same key
        self.assertEqual(resp.partition, partition)
        # expect offset +1
        self.assertEqual(resp.offset, offset + 1)

        value[23] = '*VALUE'*800
        with self.assertRaises(MessageSizeTooLargeError):
            yield from producer.send(self.topic, value, key=key)

        yield from producer.stop()
        yield from producer.stop()  # shold be Ok

    @run_until_complete
    def test_producer_send_with_compression(self):
        with self.assertRaises(ValueError):
            producer = AIOKafkaProducer(
                loop=self.loop, compression_type='my_custom')

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type='gzip')

        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)

        future = yield from producer.send(
            self.topic, b'this msg is compressed by client')
        resp = yield from future
        self.assertEqual(resp.topic, self.topic)
        self.assertTrue(resp.partition in (0, 1))
        yield from producer.stop()

    @run_until_complete
    def test_producer_send_leader_notfound(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            request_timeout_ms=200)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = -1
            future = yield from producer.send(self.topic, b'text')
            with self.assertRaises(LeaderNotAvailableError):
                yield from future

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = None
            future = yield from producer.send(self.topic, b'text')
            with self.assertRaises(NotLeaderForPartitionError):
                yield from future

        yield from producer.stop()

    @run_until_complete
    def test_producer_send_timeout(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()

        @asyncio.coroutine
        def mocked_send(nodeid, req):
            raise KafkaTimeoutError()

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send

            fut1 = yield from producer.send(self.topic, b'text1')
            fut2 = yield from producer.send(self.topic, b'text2')
            done, _ = yield from asyncio.wait([fut1, fut2], loop=self.loop)
            for item in done:
                with self.assertRaises(KafkaTimeoutError):
                    item.result()

    @run_until_complete
    def test_producer_send_error(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            retry_backoff_ms=100,
            linger_ms=5, request_timeout_ms=400)
        yield from producer.start()

        @asyncio.coroutine
        def mocked_send(nodeid, req):
            # RequestTimedOutCode error for partition=0
            return ProduceResponse([(self.topic, [(0, 7, 0), (1, 0, 111)])])

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send
            fut1 = yield from producer.send(self.topic, b'text1', partition=0)
            fut2 = yield from producer.send(self.topic, b'text2', partition=1)
            with self.assertRaises(RequestTimedOutError):
                yield from fut1
            resp = yield from fut2
            self.assertEqual(resp.offset, 111)

        @asyncio.coroutine
        def mocked_send_with_sleep(nodeid, req):
            # RequestTimedOutCode error for partition=0
            yield from asyncio.sleep(0.1, loop=self.loop)
            return ProduceResponse([(self.topic, [(0, 7, 0)])])

        with mock.patch.object(producer.client, 'send') as mocked:
            mocked.side_effect = mocked_send_with_sleep
            with self.assertRaises(RequestTimedOutError):
                future = yield from producer.send(
                    self.topic, b'text1', partition=0)
                yield from future
