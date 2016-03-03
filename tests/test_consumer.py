import asyncio
from itertools import chain
from aiokafka import SimpleAIOConsumer
from kafka import create_message

from kafka.common import ProduceRequest, ConsumerFetchSizeTooSmall
from kafka.consumer.base import MAX_FETCH_BUFFER_SIZE_BYTES

from ._testutil import KafkaIntegrationTestCase, run_until_complete, \
    random_string


class TestConsumerIntegration(KafkaIntegrationTestCase):

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
        consumer = yield from self.consumer_factory()
        # make sure that we start with no pending messages
        initial_pending = yield from consumer.pending()
        pending_part1 = yield from consumer.pending(partitions=[0])
        pending_part2 = yield from consumer.pending(partitions=[1])
        self.assertEqual(initial_pending, 0)
        self.assertEqual(pending_part1, 0)
        self.assertEqual(pending_part2, 0)

        # Produce 10 messages to partitions 0 and 1
        yield from self.send_messages(0, range(0, 10))
        yield from self.send_messages(1, range(10, 20))

        # make sure we have 20 pending message and 10 in each partition
        pending = yield from consumer.pending()
        self.assertEquals(pending, 20)
        pending_part1 = yield from consumer.pending(partitions=[0])
        pending_part2 = yield from consumer.pending(partitions=[1])
        self.assertEquals(pending_part1, 10)
        self.assertEquals(pending_part2, 10)

        # move to last message, so one partition should have 1 pending
        # message and other 0
        yield from consumer.seek(-1, 2)
        pending = yield from consumer.pending()
        pending_part1 = yield from consumer.pending(partitions=[0])
        pending_part2 = yield from consumer.pending(partitions=[1])
        self.assertEqual(pending, 1)
        self.assertEqual({0, 1}, {pending_part1, pending_part2})

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

        output_msgs1 = yield from consumer1.get_messages(185)

        consumer2 = yield from self.consumer_factory(
            auto_commit_every_t=None,
            auto_commit_every_n=20,
        )

        output_msgs2 = yield from consumer2.get_messages(15)
        result = [x.message.value for x in chain(output_msgs1, output_msgs2)]

        self.assertEqual(set(available_msgs), set(result))
        yield from consumer1.stop()
        yield from consumer2.stop()
