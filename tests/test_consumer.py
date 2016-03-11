import asyncio
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer
from aiokafka.fetcher import RecordTooLargeError

from kafka.common import TopicPartition, IllegalStateError
from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, random_string)


class TestConsumerIntegration(KafkaIntegrationTestCase):
    @asyncio.coroutine
    def send_messages(self, partition, messages):
        ret = []
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        try:
            yield from self.wait_topic(producer.client, self.topic)
            for msg in messages:
                if isinstance(msg, str):
                    msg = msg.encode()
                elif isinstance(msg, int):
                    msg = str(msg).encode()
                resp = yield from producer.send(
                    self.topic, msg, partition=partition)
                self.assertEqual(len(resp.topics), 1)
                self.assertEqual(resp.topics[0][1][0][0], partition)
                ret.append(msg)
        finally:
            yield from producer.stop()
        return ret

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEquals(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEquals(len(set(messages)), num_messages)

    @asyncio.coroutine
    def consumer_factory(self, **kwargs):
        kwargs.setdefault('enable_auto_commit', True)
        kwargs.setdefault('auto_offset_reset', 'earliest')
        group = kwargs.pop('group', 'group-%s' % self.id())
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop, group_id=group,
            bootstrap_servers=self.hosts,
            **kwargs)
        yield from consumer.start()
        yield from consumer.seek_to_committed()
        return consumer

    @run_until_complete
    def test_simple_consumer(self):
        with self.assertRaises(ValueError):
            # check unsupported version
            consumer = yield from self.consumer_factory(api_version="0.8")

        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = yield from self.consumer_factory()

        p0 = TopicPartition(self.topic, 0)
        p1 = TopicPartition(self.topic, 1)
        assignment = consumer.assignment()
        self.assertEqual(sorted(list(assignment)), [p0, p1])

        topics = yield from consumer.topics()
        self.assertTrue(self.topic in topics)

        parts = consumer.partitions_for_topic(self.topic)
        self.assertEqual(sorted(list(parts)), [0, 1])

        offset = yield from consumer.committed(
            TopicPartition("uknown-topic", 2))
        self.assertEqual(offset, None)

        offset = yield from consumer.committed(p0)
        if offset is None:
            offset = 0

        messages = []
        for i in range(200):
            message = yield from consumer.get_message()
            messages.append(message)
        self.assert_message_count(messages, 200)

        h = consumer.highwater(p0)
        self.assertEqual(h, 100)

        consumer.seek(p0, offset+90)
        for i in range(10):
            m = yield from consumer.get_message()
            self.assertEqual(m.value, str(i+90).encode())
        yield from consumer.stop()

        # will ignore, no exception expected
        yield from consumer.stop()

    @run_until_complete
    def test_consumer_poll(self):
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))
        # Start a consumer_factory
        consumer = yield from self.consumer_factory()

        messages = []
        while True:
            resp = yield from consumer.poll(1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) == 200:
                break
        self.assert_message_count(messages, 200)

        p1 = (self.topic, 1)
        consumer.pause(p1)
        yield from self.send_messages(0, list(range(0, 100)))
        yield from self.send_messages(1, list(range(100, 200)))

        messages = []
        while True:
            resp = yield from consumer.poll(1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) == 100:
                break
        self.assert_message_count(messages, 100)

        consumer.resume(p1)
        while True:
            resp = yield from consumer.poll(1000)
            for partition, msg_list in resp.items():
                messages += msg_list
            if len(messages) == 200:
                break
        self.assert_message_count(messages, 200)

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
        actual_messages = []
        for i in range(20):
            m = yield from consumer.get_message()
            actual_messages.append(m)
        actual_messages = {m.value for m in actual_messages}
        self.assertEqual(expected_messages, set(actual_messages))
        yield from consumer.stop()

    @run_until_complete
    def test_too_large_messages(self):
        l_msgs = [random_string(10), random_string(50000)]
        large_messages = yield from self.send_messages(0, l_msgs)
        r_msgs = [random_string(50)]
        small_messages = yield from self.send_messages(0, r_msgs)

        consumer = yield from self.consumer_factory(
            max_partition_fetch_bytes=4000)
        m = yield from consumer.get_message()
        self.assertEqual(m.value, large_messages[0])

        with self.assertRaises(RecordTooLargeError):
            yield from consumer.get_message()

        m = yield from consumer.get_message()
        self.assertEqual(m.value, small_messages[0])
        yield from consumer.stop()

    @run_until_complete
    def test_offset_behavior__resuming_behavior(self):
        msgs1 = yield from self.send_messages(0, range(0, 100))
        msgs2 = yield from self.send_messages(1, range(100, 200))

        available_msgs = msgs1 + msgs2
        # Start a consumer_factory
        consumer1 = yield from self.consumer_factory()
        consumer2 = yield from self.consumer_factory()
        result = []
        for i in range(10):
            msg = yield from consumer1.get_message()
            result.append(msg.value)
        yield from consumer1.stop()

        # consumer2 should take both partitions after rebalance
        while True:
            msg = yield from consumer2.get_message()
            result.append(msg.value)
            if len(result) == len(available_msgs):
                break

        yield from consumer2.stop()
        if consumer1._api_version < (0, 9):
            # coordinator rebalance feature works with >=Kafka-0.9 only
            return
        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    def test_manual_subscribe(self):
        msgs1 = yield from self.send_messages(0, range(0, 10))
        msgs2 = yield from self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = yield from self.consumer_factory()
        pos = yield from consumer.position(TopicPartition(self.topic, 0))
        with self.assertRaises(IllegalStateError):
            consumer.assign([TopicPartition(self.topic, 0)])
        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 0)])
        result = []
        for i in range(10):
            msg = yield from consumer.get_message()
            result.append(msg.value)
        self.assertEqual(set(result), set(msgs1))
        yield from consumer.commit()
        pos = yield from consumer.position(TopicPartition(self.topic, 0))
        self.assertTrue(pos > 0)

        consumer.unsubscribe()
        consumer.assign([TopicPartition(self.topic, 1)])
        for i in range(10):
            msg = yield from consumer.get_message()
            result.append(msg.value)
        yield from consumer.stop()
        self.assertEqual(set(available_msgs), set(result))

    @run_until_complete
    def test_manual_subscribe_pattern(self):
        msgs1 = yield from self.send_messages(0, range(0, 10))
        msgs2 = yield from self.send_messages(1, range(10, 20))
        available_msgs = msgs1 + msgs2

        consumer = AIOKafkaConsumer(
            loop=self.loop, group_id='test-group',
            bootstrap_servers=self.hosts, auto_offset_reset='earliest')
        consumer.subscribe(pattern="topic-test_manual_subs*")
        yield from consumer.start()
        yield from consumer.seek_to_committed()
        result = []
        for i in range(20):
            msg = yield from consumer.get_message()
            result.append(msg.value)
        yield from consumer.stop()
        self.assertEqual(set(available_msgs), set(result))
