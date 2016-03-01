import asyncio
import string
import random
import time
import unittest
import uuid
import pytest

from functools import wraps
from kafka.common import OffsetRequest, KafkaUnavailableError

from aiokafka.client import connect


__all__ = ['KafkaIntegrationTestCase', 'random_string']


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), 15, loop=loop))
        return ret
    return wrapper


@pytest.mark.usefixtures('setup_test_class', 'kafka_server')
class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """

    @pytest.fixture(autouse=True)
    def setup_kafka_host_and_port(self, kafka_server):
        self.kafka_host, self.kafka_port = kafka_server


class KafkaIntegrationTestCase(BaseTest):

    topic = None

    def setUp(self):
        super().setUp()
        self.hosts = ['{}:{}'.format(self.kafka_host, self.kafka_port)]
        for i in range(500):
            try:
                self.client = self.loop.run_until_complete(
                    connect(self.hosts, loop=self.loop))
                break
            except KafkaUnavailableError:
                time.sleep(0.1)

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:],
                               random_string(10).decode('utf-8'))
            self.topic = topic.encode('utf-8')

        self.loop.run_until_complete(
            self.client.ensure_topic_exists(self.topic))
        self._messages = {}

    def tearDown(self):
        self.client.close()
        del self.client
        super().tearDown()

    @asyncio.coroutine
    def current_offset(self, topic, partition):
        offsets, = yield from self.client.send_offset_request(
            [OffsetRequest(topic, partition, -1, 1)])
        return offsets.offsets[0]

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
