import asyncio
import string
import random
import time
import unittest
import uuid
import pytest

from functools import wraps

from kafka.common import ConnectionError
from aiokafka.client import AIOKafkaClient
from aiokafka.producer import AIOKafkaProducer


__all__ = ['KafkaIntegrationTestCase', 'random_string']


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 30, loop=loop))
        return ret
    return wrapper


@pytest.mark.usefixtures('setup_test_class')
class KafkaIntegrationTestCase(unittest.TestCase):

    topic = None
    hosts = []

    @classmethod
    def wait_kafka(cls):
        cls.hosts = ['{}:{}'.format(cls.kafka_host, cls.kafka_port)]

        # Reconnecting until Kafka in docker becomes available
        client = AIOKafkaClient(
            loop=cls.loop, bootstrap_servers=cls.hosts)

        for i in range(500):
            try:
                cls.loop.run_until_complete(client.bootstrap())
            except ConnectionError:
                time.sleep(0.1)
            else:
                cls.loop.run_until_complete(client.close())
                break

    def setUp(self):
        super().setUp()
        self._messages = {}
        if not self.topic:
            self.topic = "topic-{}-{}".format(
                self.id()[self.id().rindex(".") + 1:],
                random_string(10).decode('utf-8'))

    def tearDown(self):
        super().tearDown()

    @asyncio.coroutine
    def wait_topic(self, client, topic):
        client.add_topic(topic)
        for i in range(5):
            ok = yield from client.force_metadata_update()
            if ok:
                ok = topic in client.cluster.topics()
            if not ok:
                yield from asyncio.sleep(1, loop=self.loop)
            else:
                return
        raise AssertionError('No topic "{}" exists'.format(topic))

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
                future = yield from producer.send(
                    self.topic, msg, partition=partition)
                resp = yield from future
                self.assertEqual(resp.topic, self.topic)
                self.assertEqual(resp.partition, partition)
                ret.append(msg)
        finally:
            yield from producer.stop()
        return ret

    def assert_message_count(self, messages, num_messages):
        # Make sure we got them all
        self.assertEquals(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEquals(len(set(messages)), num_messages)

    def msgs(self, iterable):
        return [self.msg(x) for x in iterable]

    def msg(self, s):
        if s not in self._messages:
            self._messages[s] = '%s-%s-%s' % (s, self.id(), str(uuid.uuid4()))

        return self._messages[s].encode('utf-8')

    def key(self, k):
        return k.encode('utf-8')


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
