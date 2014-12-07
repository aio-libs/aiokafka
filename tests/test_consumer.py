import asyncio
import unittest
from aiokafka import SimpleAIOConsumer
from kafka import create_message

from kafka.common import ProduceRequest, ConsumerFetchSizeTooSmall
from kafka.consumer.base import MAX_FETCH_BUFFER_SIZE_BYTES

from .fixtures import ZookeeperFixture, KafkaFixture
from ._testutil import KafkaIntegrationTestCase, run_until_complete, \
    random_string


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

    @asyncio.coroutine
    def consumer_factory(self, **kwargs):

        kwargs.setdefault('auto_commit', True)
        group = kwargs.pop('group', self.id().encode('utf-8'))
        consumer = SimpleAIOConsumer(self.client, group, self.topic, **kwargs)
        yield from consumer._connect()
        return consumer

    @run_until_complete
    def test_simple_consumer(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = yield from self.consumer_factory()
        messages = yield from consumer.get_messages(200)
        self.assert_message_count(messages, 200)
        yield from consumer.stop()

    @run_until_complete
    def test_simple_consumer__seek(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        consumer = yield from self.consumer_factory()
        # Rewind 10 messages from the end
        yield from consumer.seek(-10, 2)
        messages1 = yield from consumer.get_messages(10)
        # Rewind 13 messages from the end
        yield from consumer.seek(-13, 2)
        messages2 = yield from consumer.get_messages(13)

        messages1_set = {m.message.value for m in messages1}
        messages2_set = {m.message.value for m in messages2}
        self.assertTrue(messages1_set.issubset(messages2_set))
        yield from consumer.stop()

    @run_until_complete
    def test_simple_consumer_pending(self):
        # Produce 10 messages to partitions 0 and 1
        yield from self.send_messages(0, range(0, 10))
        yield from self.send_messages(1, range(10, 20))

        consumer = yield from self.consumer_factory()

        pending = yield from consumer.pending()
        self.assertEquals(pending, 20)
        pending_part1 = yield from consumer.pending(partitions=[0])
        pending_part2 = yield from consumer.pending(partitions=[1])
        self.assertEquals(pending_part1, 10)
        self.assertEquals(pending_part2, 10)
        yield from consumer.stop()

    @run_until_complete
    def test_large_messages(self):
        # Produce 10 "normal" size messages
        r_msgs = [str(x) for x in range(10)]
        small_messages = yield from self.send_messages(0, r_msgs)

        # Produce 10 messages that are large (bigger than default fetch size)
        l_msgs = [random_string(5000) for _ in range(10)]
        large_messages = yield from self.send_messages(0, l_msgs)

        # Consumer should still get all of them
        consumer = yield from self.consumer_factory()
        expected_messages = set(small_messages + large_messages)
        actual_messages = yield from consumer.get_messages(20)
        actual_messages = {m.message.value for m in actual_messages}
        self.assertEqual(expected_messages, set(actual_messages))
        yield from consumer.stop()

    @run_until_complete
    def test_huge_messages(self):
        h_msg = create_message(random_string(MAX_FETCH_BUFFER_SIZE_BYTES + 10))
        (huge_message,) = yield from self.send_messages(0, [h_msg])

        # Create a consumer_factory with the default buffer size
        consumer = yield from self.consumer_factory()

        # This consumer_factory failes to get the message
        with self.assertRaises(ConsumerFetchSizeTooSmall):
            yield from consumer.get_messages(1)
        yield from consumer.stop()

        # Create a consumer_factory with no fetch size limit
        big_consumer = yield from self.consumer_factory(
            max_buffer_size=None, partitions=[0])

        # Seek to the last message
        # TODO: fix or remove
        # yield from big_consumer.seek(-1, 2)

        # Consume giant message successfully
        (message,) = yield from big_consumer.get_messages(1)
        self.assertIsNotNone(message)
        self.assertEquals(message.message.value, huge_message)
        yield from big_consumer.stop()

    @unittest.skip("not ported")
    @run_until_complete
    def test_offset_behavior__resuming_behavior(self):
        msgs1 = yield from self.send_messages(0, range(0, 100))
        msgs2 = yield from self.send_messages(1, range(100, 200))

        available_msgs = msgs1 + msgs2
        # Start a consumer_factory
        consumer1 = yield from self.consumer_factory(
            auto_commit_every_t=None,
            auto_commit_every_n=20,
        )

        # Grab the first 195 messages
        output_msgs1 = yield from consumer1.get_messages(195)

        # The total offset across both partitions should be at 180
        consumer2 = yield from self.consumer_factory(
            auto_commit_every_t=None,
            auto_commit_every_n=20,
        )

        # 181-200
        output_msgs2 = yield from consumer2.get_messages(20)

        self.assertAlmostEqual(set(available_msgs),
                               set(output_msgs1 + output_msgs2))
        yield from consumer1.stop()
        yield from consumer2.stop()

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

        consumer = self.consumer_factory(buffer_size=1024,
                                         max_buffer_size=2048)
        messages = [message for message in consumer]
        self.assertEquals(len(messages), 2)
