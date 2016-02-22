import json
from unittest import mock

from kafka.cluster import ClusterMetadata
from kafka.common import (UnknownTopicOrPartitionError,
                          MessageSizeTooLargeError,
                          NotLeaderForPartitionError,
                          LeaderNotAvailableError)

from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka.producer import AIOKafkaProducer


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = 'test_produce_topic'

    @run_until_complete
    def test_producer_start(self):
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
        resp = yield from producer.send(self.topic, b'hello, Kafka!')
        self.assertEqual(len(resp.topics), 1)
        self.assertEqual(resp.topics[0][0], self.topic)
        self.assertEqual(len(resp.topics[0][1]), 1)
        self.assertTrue(resp.topics[0][1][0][0] in (0, 1))  # partition
        self.assertEqual(resp.topics[0][1][0][1], 0)  # error code
        self.assertEqual(resp.topics[0][1][0][2], 0)  # offset

        resp = yield from producer.send(self.topic, b'second msg', partition=1)
        self.assertEqual(resp.topics[0][1][0][0], 1)  # partition

        resp = yield from producer.send(self.topic, b'value', key=b'KEY')
        self.assertTrue(resp.topics[0][1][0][0] in (0, 1))  # partition
        self.assertEqual(resp.topics[0][1][0][1], 0)  # error code
        yield from producer.stop()

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
        resp = yield from producer.send(self.topic, value, key=key)
        partition = resp.topics[0][1][0][0]
        offset = resp.topics[0][1][0][2]
        self.assertTrue(partition in (0, 1))  # partition
        self.assertEqual(resp.topics[0][1][0][1], 0)  # error code

        resp = yield from producer.send(self.topic, 'some str', key=key)
        self.assertEqual(resp.topics[0][1][0][1], 0)  # error code
        # expect the same partition bcs the same key
        self.assertEqual(resp.topics[0][1][0][0], partition)
        # expect offset +1
        self.assertEqual(resp.topics[0][1][0][2], offset + 1)

        value[23] = '*VALUE'*800
        with self.assertRaises(MessageSizeTooLargeError):
            resp = yield from producer.send(self.topic, value, key=key)

        yield from producer.stop()
        yield from producer.stop()  # shold be Ok

    @run_until_complete
    def test_producer_send_with_compression(self):
        with self.assertRaises(AssertionError):
            producer = AIOKafkaProducer(
                loop=self.loop, compression_type='my_custom')

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            compression_type='gzip')

        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)

        resp = yield from producer.send(
            self.topic, b'this msg is compressed by client')
        self.assertEqual(len(resp.topics), 1)
        self.assertEqual(resp.topics[0][0], self.topic)
        self.assertEqual(len(resp.topics[0][1]), 1)
        self.assertTrue(resp.topics[0][1][0][0] in (0, 1))  # partition
        self.assertEqual(resp.topics[0][1][0][1], 0)  # error code
        yield from producer.stop()

    @run_until_complete
    def test_producer_send_leader_notfound(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        yield from self.wait_topic(producer.client, self.topic)

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = -1
            with self.assertRaises(LeaderNotAvailableError):
                yield from producer.send(self.topic, b'text')

        with mock.patch.object(
                ClusterMetadata, 'leader_for_partition') as mocked:
            mocked.return_value = None
            with self.assertRaises(NotLeaderForPartitionError):
                yield from producer.send(self.topic, b'text')

        yield from producer.stop()
