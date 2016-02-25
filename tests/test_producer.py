import asyncio
import uuid
import unittest
from unittest import mock

from kafka import (
    create_message, create_gzip_message, create_snappy_message,
    RoundRobinPartitioner, HashedPartitioner
)
from kafka.common import (
    FetchRequest, ProduceRequest,
    UnknownTopicOrPartitionError, LeaderNotAvailableError,
    UnsupportedCodecError
)

from ._testutil import KafkaIntegrationTestCase, run_until_complete

from aiokafka.producer import (SimpleAIOProducer, KeyedAIOProducer,
                               CODEC_SNAPPY, CODEC_GZIP)
from aiokafka.client import AIOKafkaClient


class TestKafkaProducer(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()

    def test_invalid_codec(self):
        client = mock.Mock()
        with self.assertRaises(UnsupportedCodecError):
            SimpleAIOProducer(client, codec=123)

    def test_send_non_byteish(self):
        client = mock.Mock()
        sproducer = SimpleAIOProducer(client)
        with self.assertRaises(TypeError):
            self.loop.run_until_complete(sproducer.send(b"topic", "text"))

        kproducer = KeyedAIOProducer(client)
        with self.assertRaises(TypeError):
            self.loop.run_until_complete(kproducer.send(b"topic", "text",
                                                        key=b'key'))

    def test_send_non_byteish_key(self):
        client = mock.Mock()
        producer = KeyedAIOProducer(client)
        with self.assertRaises(TypeError):
            self.loop.run_until_complete(producer.send(b"topic", b"text",
                                                       key='key'))

    def test_simple_producer_ctor(self):
        client = mock.Mock()
        ack = SimpleAIOProducer.ACK_AFTER_LOCAL_WRITE
        producer = SimpleAIOProducer(client, req_acks=ack)
        name = producer.__class__.__name__
        self.assertTrue(name in producer.__repr__())
        self.assertEqual(producer._req_acks, ack)

    def test_keyed_producer_ctor(self):
        client = mock.Mock()
        ack = KeyedAIOProducer.ACK_AFTER_LOCAL_WRITE

        producer = KeyedAIOProducer(client,
                                    partitioner=RoundRobinPartitioner,
                                    req_acks=ack)
        name = producer.__class__.__name__
        self.assertTrue(name in producer.__repr__())
        self.assertEqual(producer._req_acks, ack)


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):
    topic = b'produce_topic'

    @run_until_complete
    def test_produce_many_simple(self):
        start_offset = yield from self.current_offset(self.topic, 0)
        msgs1 = [create_message(("Test message %d" % i).encode('utf-8'))
                 for i in range(100)]

        yield from self.assert_produce_request(msgs1, start_offset, 100)

        msgs2 = [create_message(("Test message %d" % i).encode('utf-8'))
                 for i in range(100)]
        yield from self.assert_produce_request(msgs2, start_offset + 100, 100)

    @run_until_complete
    def test_produce_10k_simple(self):
        start_offset = yield from self.current_offset(self.topic, 0)
        msgs = [create_message(("Test message %d" % i).encode('utf-8'))
                for i in range(10000)]
        yield from self.assert_produce_request(msgs, start_offset, 10000)

    @run_until_complete
    def test_produce_many_gzip(self):
        start_offset = yield from self.current_offset(self.topic, 0)

        message1 = create_gzip_message([
            ("Gzipped 1 %d" % i).encode('utf-8') for i in range(100)])
        message2 = create_gzip_message([
            ("Gzipped 2 %d" % i).encode('utf-8') for i in range(100)])

        yield from self.assert_produce_request([message1, message2],
                                               start_offset, 200)

    @run_until_complete
    def test_produce_many_snappy(self):
        start_offset = yield from self.current_offset(self.topic, 0)
        msgs1 = create_snappy_message(
            [b"Snappy 1" + bytes(i) for i in range(100)])
        msgs2 = create_snappy_message(
            [b"Snappy 2" + bytes(i) for i in range(100)])
        yield from self.assert_produce_request([msgs1, msgs2], start_offset,
                                               200)

    @run_until_complete
    def test_produce_mixed(self):
        start_offset = yield from self.current_offset(self.topic, 0)

        msg_count = 1 + 100 + 100
        messages = [
            create_message(b"Just a plain message"),
            create_gzip_message([
                ("Gzipped %d" % i).encode('utf-8') for i in range(100)]),
            create_snappy_message([
                b"Snappy " + bytes(i) for i in range(100)])]

        yield from self.assert_produce_request(messages, start_offset,
                                               msg_count)

    @run_until_complete
    def test_produce_100k_gzipped(self):
        start_offset = yield from self.current_offset(self.topic, 0)

        msgs1 = [("Gzipped batch 1, message %d" % i).encode('utf-8')
                 for i in range(50000)]
        yield from self.assert_produce_request([create_gzip_message(msgs1)],
                                               start_offset, 50000, )

        msgs2 = [("Gzipped batch 1, message %d" % i).encode('utf-8')
                 for i in range(50000)]

        yield from self.assert_produce_request([create_gzip_message(msgs2)],
                                               start_offset + 50000, 50000, )

    # SimpleProducer Tests

    @run_until_complete
    def test_simple_producer(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)
        start_offset1 = yield from self.current_offset(self.topic, 1)
        producer = SimpleAIOProducer(self.client)

        # Goes to first partition, randomly.
        resp = yield from producer.send(self.topic, self.msg("one"),
                                        self.msg("two"))
        self.assert_produce_response(resp, start_offset0)

        # Goes to the next partition, randomly.
        resp = yield from producer.send(self.topic, self.msg("three"))
        self.assert_produce_response(resp, start_offset1)

        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one"), self.msg("two")])
        yield from self.assert_fetch_offset(1, start_offset1,
                                            [self.msg("three")])

        # Goes back to the first partition because there's only two partitions
        resp = yield from producer.send(self.topic, self.msg("four"),
                                        self.msg("five"))
        self.assert_produce_response(resp, start_offset0 + 2)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one"), self.msg("two"),
                                             self.msg("four"),
                                             self.msg("five")])

    @run_until_complete
    def test_produce__new_topic_fails_with_reasonable_error(self):
        new_topic = 'new_topic_{guid}'.format(guid=str(uuid.uuid4())).encode(
            'utf-8')
        producer = SimpleAIOProducer(self.client)

        # At first it doesn't exist
        with self.assertRaises((UnknownTopicOrPartitionError,
                                LeaderNotAvailableError)):
            yield from producer.send(new_topic, self.msg("one"))

    @run_until_complete
    def test_producer_random_order(self):
        producer = SimpleAIOProducer(self.client, random_start=True)
        resp1 = yield from producer.send(self.topic, self.msg("one"),
                                         self.msg("two"))
        resp2 = yield from producer.send(self.topic,
                                         self.msg("three"))
        resp3 = yield from producer.send(self.topic, self.msg("four"),
                                         self.msg("five"))

        self.assertEqual(resp1[0].partition, resp3[0].partition)
        self.assertNotEqual(resp1[0].partition, resp2[0].partition)

    @run_until_complete
    def test_producer_ordered_start(self):
        producer = SimpleAIOProducer(self.client, random_start=False)
        resp1 = yield from producer.send(self.topic, self.msg("one"),
                                         self.msg("two"))
        resp2 = yield from producer.send(self.topic,
                                         self.msg("three"))
        resp3 = yield from producer.send(self.topic, self.msg("four"),
                                         self.msg("five"))

        self.assertEqual(resp1[0].partition, 0)
        self.assertEqual(resp2[0].partition, 1)
        self.assertEqual(resp3[0].partition, 0)

    @run_until_complete
    def test_codec_gzip(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)
        producer = SimpleAIOProducer(
            self.client, codec=CODEC_GZIP)
        resp = yield from producer.send(self.topic, self.msg("one"))
        self.assert_produce_response(resp, start_offset0)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one")])

    @run_until_complete
    def test_codec_snappy(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)

        producer = SimpleAIOProducer(
            self.client, codec=CODEC_SNAPPY)
        resp = yield from producer.send(self.topic, self.msg("one"))
        self.assert_produce_response(resp, start_offset0)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one")])

    @run_until_complete
    def test_acks_none(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)

        producer = SimpleAIOProducer(
            self.client, req_acks=SimpleAIOProducer.ACK_NOT_REQUIRED)
        resp = yield from producer.send(self.topic, self.msg("one"))
        self.assertEquals(len(resp), 0)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one")])

    @run_until_complete
    def test_acks_local_write(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)

        producer = SimpleAIOProducer(
            self.client, req_acks=SimpleAIOProducer.ACK_AFTER_LOCAL_WRITE)
        resp = yield from producer.send(self.topic, self.msg("one"))

        self.assert_produce_response(resp, start_offset0)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one")])

    @run_until_complete
    def test_acks_cluster_commit(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)

        producer = SimpleAIOProducer(
            self.client,
            req_acks=SimpleAIOProducer.ACK_AFTER_CLUSTER_COMMIT)

        resp = yield from producer.send(self.topic, self.msg("one"))
        self.assert_produce_response(resp, start_offset0)
        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one")])

    # KeyedAIOProducer Tests

    @run_until_complete
    def test_round_robin_partitioner(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)
        start_offset1 = yield from self.current_offset(self.topic, 1)

        producer = KeyedAIOProducer(self.client,
                                    partitioner=RoundRobinPartitioner)
        resp1 = yield from producer.send(self.topic,
                                         self.msg("one"),
                                         key=self.key("key1"))
        resp2 = yield from producer.send(self.topic,
                                         self.msg("two"),
                                         key=self.key("key2"))
        resp3 = yield from producer.send(self.topic,
                                         self.msg("three"),
                                         key=self.key("key3"))
        resp4 = yield from producer.send(self.topic,
                                         self.msg("four"),
                                         key=self.key("key4"))

        self.assert_produce_response(resp1, start_offset0 + 0)
        self.assert_produce_response(resp2, start_offset1 + 0)
        self.assert_produce_response(resp3, start_offset0 + 1)
        self.assert_produce_response(resp4, start_offset1 + 1)

        yield from self.assert_fetch_offset(0, start_offset0,
                                            [self.msg("one"),
                                             self.msg("three")])
        yield from self.assert_fetch_offset(1, start_offset1,
                                            [self.msg("two"),
                                             self.msg("four")])

    @run_until_complete
    def test_hashed_partitioner(self):
        start_offset0 = yield from self.current_offset(self.topic, 0)
        start_offset1 = yield from self.current_offset(self.topic, 1)

        # create client without preloaded metadata, and enforce
        # metadata loading on first call of sent method
        client = AIOKafkaClient(self.hosts, loop=self.loop)
        producer = KeyedAIOProducer(client, partitioner=HashedPartitioner)
        resp1 = yield from producer.send(self.topic,
                                         self.msg("one"),
                                         key=self.key("1"),)
        resp2 = yield from producer.send(self.topic,
                                         self.msg("two"),
                                         key=self.key("2"),)
        resp3 = yield from producer.send(self.topic,
                                         self.msg("three"),
                                         key=self.key("3"))
        resp4 = yield from producer.send(self.topic,
                                         self.msg("four"),
                                         key=self.key("3"))
        resp5 = yield from producer.send(self.topic,
                                         self.msg("five"),
                                         key=self.key("4"),)

        offsets = {0: start_offset0, 1: start_offset1}
        messages = {0: [], 1: []}

        keys = [self.key(k) for k in ["1", "2", "3", "3", "4"]]
        resps = [resp1, resp2, resp3, resp4, resp5]
        msgs = [self.msg(m) for m in ["one", "two", "three", "four", "five"]]

        for key, resp, msg in zip(keys, resps, msgs):
            k = hash(key) % 2
            offset = offsets[k]
            self.assert_produce_response(resp, offset)
            offsets[k] += 1
            messages[k].append(msg)

        yield from self.assert_fetch_offset(0, start_offset0, messages[0])
        yield from self.assert_fetch_offset(1, start_offset1, messages[1])

    # test helpers

    @asyncio.coroutine
    def assert_produce_request(self, messages, initial_offset, message_ct):
        produce = ProduceRequest(self.topic, 0, messages=messages)

        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.
        resp = yield from self.client.send_produce_request([produce])
        self.assert_produce_response(resp, initial_offset)
        offset = yield from self.current_offset(self.topic, 0)
        self.assertEqual(offset, initial_offset + message_ct)

    def assert_produce_response(self, resp, initial_offset):
        self.assertEqual(len(resp), 1)
        self.assertEqual(resp[0].error, 0)
        self.assertEqual(resp[0].offset, initial_offset)

    @asyncio.coroutine
    def assert_fetch_offset(self, partition, start_offset, expected_msgs):
        # There should only be one response message from the server.
        # This will throw an exception if there's more than one.

        resp, = yield from self.client.send_fetch_request(
            [FetchRequest(self.topic, partition, start_offset, 1024)])

        self.assertEquals(resp.error, 0)
        self.assertEquals(resp.partition, partition)
        messages = [x.message.value for x in resp.messages]

        self.assertEqual(messages, expected_msgs)
        self.assertEquals(resp.highwaterMark,
                          start_offset + len(expected_msgs))
