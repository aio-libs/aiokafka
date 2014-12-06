import asyncio
import string
import os
import random
import unittest
from aiokafka import SimpleAIOConsumer
from kafka import create_message

from kafka.common import ProduceRequest, ConsumerFetchSizeTooSmall
from kafka.consumer.base import MAX_FETCH_BUFFER_SIZE_BYTES

from .fixtures import ZookeeperFixture, KafkaFixture
from ._testutil import KafkaIntegrationTestCase, run_until_complete


class TestConsumerIntegration(KafkaIntegrationTestCase):

    @classmethod
    def setUpClass(cls):
        cls.zk = ZookeeperFixture.instance()
        cls.server1 = KafkaFixture.instance(0, cls.zk.host, cls.zk.port)
        cls.server2 = KafkaFixture.instance(1, cls.zk.host, cls.zk.port)

        cls.server = cls.server1  # Bootstrapping server

    @classmethod
    def tearDownClass(cls):
        cls.server1.close()
        cls.server2.close()
        cls.zk.close()

    @asyncio.coroutine
    def send_messages(self, partition, messages):
        messages = [create_message(self.msg(str(msg))) for msg in messages]
        produce = ProduceRequest(self.topic, partition, messages=messages)
        resp, = yield from self.client.send_produce_request([produce])
        self.assertEquals(resp.error, 0)

        return [x.value for x in messages]

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEquals(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEquals(len(set(messages)), num_messages)

    def consumer_factory(self, **kwargs):
        kwargs.setdefault('auto_commit', True)
        group = kwargs.pop('group', self.id().encode('utf-8'))
        return SimpleAIOConsumer(self.client, group, self.topic, **kwargs)


    @run_until_complete
    def test_simple_consumer(self):
        yield from self.send_messages(0, range(0, 100))
        yield from self.send_messages(1, range(100, 200))
        # Start a consumer_factory
        consumer = self.consumer_factory()
        yield from consumer._connect()
        messages = []
        # for i, waiter in enumerate(consumer.fetch_messages()):
        for i in range(200):
            msg = yield from consumer.get_message()
            messages.append(msg)
            print(i)
            if i!=0 and not i % 199:
                print(i)
                break

        self.assert_message_count(messages, 200)

        yield from consumer.stop()

    @unittest.skip("not ported")
    def test_simple_consumer__seek(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        consumer = self.consumer_factory()

        # Rewind 10 messages from the end
        consumer.seek(-10, 2)
        self.assert_message_count([message for message in consumer], 10)

        # Rewind 13 messages from the end
        consumer.seek(-13, 2)
        self.assert_message_count([message for message in consumer], 13)

        consumer.stop()

    @unittest.skip("not ported")
    def test_simple_consumer_blocking(self):
        consumer = self.consumer_factory()

        # Ask for 5 messages, nothing in queue, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(block=True, timeout=5)
            self.assert_message_count(messages, 0)
        self.assertGreaterEqual(t.interval, 5)

        self.send_messages(0, range(0, 10))

        # Ask for 5 messages, 10 in queue. Get 5 back, no blocking
        with Timer() as t:
            messages = consumer.get_messages(count=5, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertLessEqual(t.interval, 1)

        # Ask for 10 messages, get 5 back, block 5 seconds
        with Timer() as t:
            messages = consumer.get_messages(count=10, block=True, timeout=5)
            self.assert_message_count(messages, 5)
        self.assertGreaterEqual(t.interval, 5)

        consumer.stop()

    @unittest.skip("not ported")
    def test_simple_consumer_pending(self):
        # Produce 10 messages to partitions 0 and 1
        self.send_messages(0, range(0, 10))
        self.send_messages(1, range(10, 20))

        consumer = self.consumer_factory()

        self.assertEquals(consumer.pending(), 20)
        self.assertEquals(consumer.pending(partitions=[0]), 10)
        self.assertEquals(consumer.pending(partitions=[1]), 10)

        consumer.stop()

    @unittest.skip("not ported")
    def test_large_messages(self):
        # Produce 10 "normal" size messages
        small_messages = self.send_messages(0, [str(x) for x in range(10)])

        # Produce 10 messages that are large (bigger than default fetch size)
        large_messages = self.send_messages(0, [random_string(5000) for x in
                                                range(10)])

        # Consumer should still get all of them
        consumer = self.consumer_factory()

        expected_messages = set(small_messages + large_messages)
        actual_messages = set([x.message.value for x in consumer])
        self.assertEqual(expected_messages, actual_messages)

        consumer.stop()

    @unittest.skip("not ported")
    def test_huge_messages(self):
        huge_message, = self.send_messages(0, [
            create_message(random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10)),
        ])

        # Create a consumer_factory with the default buffer size
        consumer = self.consumer_factory()

        # This consumer_factory failes to get the message
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            consumer.get_message(False, 0.1)

        consumer.stop()

        # Create a consumer_factory with no fetch size limit
        big_consumer = self.consumer_factory(
            max_buffer_size=None,
            partitions=[0],
        )

        # Seek to the last message
        big_consumer.seek(-1, 2)

        # Consume giant message successfully
        message = big_consumer.get_message(block=False, timeout=10)
        self.assertIsNotNone(message)
        self.assertEquals(message.message.value, huge_message)

        big_consumer.stop()

    @unittest.skip("not ported")
    def test_offset_behavior__resuming_behavior(self):
        self.send_messages(0, range(0, 100))
        self.send_messages(1, range(100, 200))

        # Start a consumer_factory
        consumer1 = self.consumer_factory(
            auto_commit_every_t=None,
            auto_commit_every_n=20,
        )

        # Grab the first 195 messages
        output_msgs1 = [consumer1.get_message().message.value for _ in
                        range(195)]
        self.assert_message_count(output_msgs1, 195)

        # The total offset across both partitions should be at 180
        consumer2 = self.consumer_factory(
            auto_commit_every_t=None,
            auto_commit_every_n=20,
        )

        # 181-200
        self.assert_message_count([message for message in consumer2], 20)

        consumer1.stop()
        consumer2.stop()

    # TODO: Make this a unit test -- should not require integration
    @unittest.skip("not ported")
    def test_fetch_buffer_size(self):
        # Test parameters (see issue 135 / PR 136)
        TEST_MESSAGE_SIZE = 1048
        INIT_BUFFER_SIZE = 1024
        MAX_BUFFER_SIZE = 2048
        assert TEST_MESSAGE_SIZE > INIT_BUFFER_SIZE
        assert TEST_MESSAGE_SIZE < MAX_BUFFER_SIZE
        assert MAX_BUFFER_SIZE == 2 * INIT_BUFFER_SIZE

        self.send_messages(0, ["x" * 1048])
        self.send_messages(1, ["x" * 1048])

        consumer = self.consumer_factory(buffer_size=1024, max_buffer_size=2048)
        messages = [message for message in consumer]
        self.assertEquals(len(messages), 2)




def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
