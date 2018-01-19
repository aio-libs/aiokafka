import asyncio
import string
import random
import time
import unittest
import pytest
import operator
import sys

from contextlib import contextmanager
from functools import wraps

from aiokafka import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import ConnectionError
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


def kafka_versions(*versions):
    # Took from kafka-python

    def version_str_to_list(s):
        return list(map(int, s.split('.')))  # e.g., [0, 8, 1, 1]

    def construct_lambda(s):
        if s[0].isdigit():
            op_str = '='
            v_str = s
        elif s[1].isdigit():
            op_str = s[0]  # ! < > =
            v_str = s[1:]
        elif s[2].isdigit():
            op_str = s[0:2]  # >= <=
            v_str = s[2:]
        else:
            raise ValueError('Unrecognized kafka version / operator: %s' % s)

        op_map = {
            '=': operator.eq,
            '!': operator.ne,
            '>': operator.gt,
            '<': operator.lt,
            '>=': operator.ge,
            '<=': operator.le
        }
        op = op_map[op_str]
        version = version_str_to_list(v_str)
        return lambda a: op(version_str_to_list(a), version)

    validators = map(construct_lambda, versions)

    def kafka_versions(func):
        @wraps(func)
        def wrapper(self, *args, **kw):
            kafka_version = self.kafka_version

            if not kafka_version:
                self.skipTest(
                    "no kafka version found. Is this an integration test?")

            for f in validators:
                if not f(kafka_version):
                    self.skipTest("unsupported kafka version")

            return func(self, *args, **kw)
        return wrapper
    return kafka_versions


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
        client = AIOKafkaClient(loop=cls.loop, bootstrap_servers=cls.hosts)
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
                return
        assert False, "Kafka server never started"

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
            task = asyncio.wait_for(coro(*args, **kw), 30, loop=self.loop)
            self.loop.run_until_complete(task)

    def add_cleanup(self, cb_or_coro, *args, **kw):
        self._cleanup.append((cb_or_coro, args, kw))

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
    def send_messages(self, partition, messages, *, topic=None,
                      timestamp_ms=None, return_inst=False):
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
                    topic, msg, partition=partition, timestamp_ms=timestamp_ms)
                resp = yield from future
                self.assertEqual(resp.topic, topic)
                self.assertEqual(resp.partition, partition)
                if return_inst:
                    ret.append(resp)
                else:
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

    def assertLogs(self, *args, **kw):  # noqa
        if sys.version_info >= (3, 4, 0):
            return super().assertLogs(*args, **kw)
        else:
            # On Python3.3 just do no-op for now
            return self._assert_logs_noop()

    @contextmanager
    def _assert_logs_noop(self):
        yield


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
