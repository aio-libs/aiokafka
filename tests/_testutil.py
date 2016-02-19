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


@pytest.mark.usefixtures('setup_test_class')
class KafkaIntegrationTestCase(unittest.TestCase):

    topic = None

    def setUp(self):
        super().setUp()
        self.hosts = ['{}:{}'.format(self.kafka_host, self.kafka_port)]

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:],
                               random_string(10).decode('utf-8'))
            self.topic = topic.encode('utf-8')

        # Reconnecting until Kafka in docker becomes available
        client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts)
        for i in range(500):
            try:
                self.loop.run_until_complete(client.bootstrap())

            except ConnectionError:
                time.sleep(0.1)
            else:
                yield from client.close()
                break
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
