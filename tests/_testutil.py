import asyncio
import string
import random
import time
import unittest
import pytest

from functools import wraps

from kafka.common import ConnectionError
from kafka.consumer.subscription_state import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.producer import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context


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


class StubRebalanceListener(ConsumerRebalanceListener):

    def __init__(self, *, loop):
        self.assigns = asyncio.Queue(loop=loop)
        self.revokes = asyncio.Queue(loop=loop)
        self.assigned = None
        self.revoked = None

    @asyncio.coroutine
    def wait_assign(self):
        return (yield from self.assigns.get())

    def reset(self):
        while not self.assigns.empty():
            self.assigns.get_nowait()
        while not self.revokes.empty():
            self.revokes.get_nowait()

    def on_partitions_revoked(self, revoked):
        self.revokes.put_nowait(revoked)

    def on_partitions_assigned(self, assigned):
        self.assigns.put_nowait(assigned)


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
                # Wait for broker to look for others.
                if not client.cluster.brokers():
                    time.sleep(0.1)
                    continue
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
        self._cleanup = []

    def tearDown(self):
        super().tearDown()
        for coro, args, kw in reversed(self._cleanup):
            self.loop.run_until_complete(coro(*args, **kw))

    def add_cleanup(self, cb_or_coro, *args, **kw):
        self._cleanup.append(
            (cb_or_coro, args, kw))

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
    def send_messages(self, partition, messages, *, topic=None):
        topic = topic or self.topic
        ret = []
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        yield from producer.start()
        try:
            yield from self.wait_topic(producer.client, topic)
            for msg in messages:
                if isinstance(msg, str):
                    msg = msg.encode()
                elif isinstance(msg, int):
                    msg = str(msg).encode()
                future = yield from producer.send(
                    topic, msg, partition=partition)
                resp = yield from future
                self.assertEqual(resp.topic, topic)
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

    def create_ssl_context(self):
        return create_ssl_context(
            cafile=str(self.ssl_folder / "ca-cert"),
            certfile=str(self.ssl_folder / "cl_client.pem"),
            keyfile=str(self.ssl_folder / "cl_client.key"),
            password="abcdefgh")


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
