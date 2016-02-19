import asyncio
import socket
import string
import random
import unittest
import uuid

from functools import wraps
from aiokafka.client import AIOKafkaClient


__all__ = ['get_open_port', 'KafkaIntegrationTestCase', 'random_string']


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


class BaseTest(unittest.TestCase):
    """Base test case for unittests.
    """
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(None)

    def tearDown(self):
        self.loop.close()
        del self.loop


class KafkaIntegrationTestCase(BaseTest):

    topic = None

    def setUp(self):
        super().setUp()
        self.hosts = ['{}:{}'.format(self.server.host, self.server.port)]
        self.client = AIOKafkaClient(
            loop=self.loop, bootstrap_servers=self.hosts)
        self.client = self.loop.run_until_complete(
            self.client.bootstrap())

        if not self.topic:
            topic = "%s-%s" % (self.id()[self.id().rindex(".") + 1:],
                               random_string(10).decode('utf-8'))
            self.topic = topic.encode('utf-8')

        update_md_task = self.client.set_topics([self.topic])
        self.loop.run_until_complete(
            asyncio.wait_for(update_md_task, loop=self.loop))
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


def get_open_port():
    sock = socket.socket()
    sock.bind(("", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
