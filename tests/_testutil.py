import asyncio
import string
import random
import time
import unittest
import pytest
import operator
import subprocess
import pathlib
import shutil
import sys
import os

from contextlib import contextmanager
from functools import wraps

from aiokafka import ConsumerRebalanceListener
from aiokafka.client import AIOKafkaClient
from aiokafka.errors import ConnectionError
from aiokafka.producer import AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

import logging
log = logging.getLogger(__name__)


__all__ = ['KafkaIntegrationTestCase', 'random_string']


def run_until_complete(fun):
    if not asyncio.iscoroutinefunction(fun):
        fun = asyncio.coroutine(fun)

    @wraps(fun)
    def wrapper(test, *args, **kw):
        loop = test.loop
        timeout = getattr(test, "TEST_TIMEOUT", 30)
        ret = loop.run_until_complete(
            asyncio.wait_for(fun(test, *args, **kw), timeout, loop=loop))
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


class ACLManager:

    def __init__(self, docker, tag):
        self._docker = docker
        self._active_acls = []
        self._tag = tag

    @property
    def cmd(self):
        return "/opt/kafka_{tag}/bin/kafka-acls.sh".format(tag=self._tag)

    def _exec(self, *cmd_options):
        cmd = ' '.join(
            [self.cmd, "--force",
              '--authorizer-properties zookeeper.connect=localhost:2181'
             ] + list(cmd_options))
        exit_code, output = self._docker.exec_run(cmd)
        if exit_code != 0:
            for line in output.split(b'\n'):
                log.warning(line)
            raise RuntimeError("Failed to apply ACL")
        else:
            for line in output.split(b'\n'):
                log.debug(line)
            return output

    def add_acl(self, **acl_params):
        params = self._format_params(**acl_params)
        self._exec("--add", *params)
        self._active_acls.append(acl_params)

    def remove_acl(self, **acl_params):
        params = self._format_params(**acl_params)
        self._exec("--remove", *params)
        self._active_acls.remove(acl_params)

    def list_acl(self, principal=None):
        opts = []
        if principal:
            opts.append("--principal User:{}".format(principal))
        return self._exec('--list', *opts)

    def _format_params(
            self, cluster=None, topic=None, group=None,
            transactional_id=None,
            allow_principal=None, deny_principal=None,
            allow_host=None, deny_host=None,
            operation=None, producer=None, consumer=None):
        options = []
        if cluster:
            options.append("--cluster")
        if topic is not None:
            options.append("--topic {}".format(topic))
        if group is not None:
            options.append("--group {}".format(group))
        if transactional_id is not None:
            options.append("--transactional-id {}".format(transactional_id))
        if allow_principal is not None:
            options.append("--allow-principal User:{}".format(allow_principal))
        if deny_principal is not None:
            options.append("--deny-principal User:{}".format(deny_principal))
        if allow_host is not None:
            options.append("--allow-host {}".format(allow_host))
        if deny_host is not None:
            options.append("--deny-host {}".format(deny_host))
        if operation is not None:
            options.append("--operation {}".format(operation))
        if producer is not None:
            options.append("--producer")
        if consumer is not None:
            options.append("--consumer")
        return options

    def cleanup(self):
        for acl_params in self._active_acls:
            self.remove_acl(**acl_params)


class KerberosUtils:

    def __init__(self, docker):
        self._docker = docker

    def create_keytab(
            self, principal="client/localhost",
            password="aiokafka",
            keytab_file="client.keytab"):

        scripts_dir = pathlib.Path("docker/scripts/krb5.conf")
        os.environ["KRB5_CONFIG"] = str(scripts_dir.absolute())

        keytab_dir = pathlib.Path('tests/keytab')
        if keytab_dir.exists():
            shutil.rmtree(str(keytab_dir))

        keytab_dir.mkdir()

        if sys.platform == 'darwin':
            subprocess.run(
                ['ktutil', '-k', keytab_file,
                 'add',
                 '-p', principal,
                 '-V', '1',
                 '-e', 'aes256-cts-hmac-sha1-96',
                 '-w', password],
                cwd=str(keytab_dir.absolute()), check=True)
        elif sys.platform != 'win32':
            input_data = (
                "add_entry -password -p {principal} -k 1 "
                "-e aes256-cts-hmac-sha1-96\n"
                "{password}\n"
                "write_kt {keytab_file}\n"
            ).format(
                principal=principal,
                password=password,
                keytab_file=keytab_file)
            subprocess.run(
                ['ktutil'],
                cwd=str(keytab_dir.absolute()),
                input=input_data, check=True)
        else:
            raise NotImplementedError

        self.keytab = keytab_dir / keytab_file

    def kinit(self, principal):
        assert self.keytab
        subprocess.run(
            ['kinit', '-kt', self.keytab.absolute(), principal],
            check=True)

    def kdestroy(self):
        assert self.keytab
        subprocess.run(
            ['kdestroy', '-A'],
            check=True)


@pytest.mark.usefixtures('setup_test_class')
class KafkaIntegrationTestCase(unittest.TestCase):

    topic = None
    hosts = []

    @classmethod
    def wait_kafka(cls):
        cls.hosts = ['{}:{}'.format(cls.kafka_host, cls.kafka_port)]

        # Reconnecting until Kafka in docker becomes available
        for i in range(500):
            client = AIOKafkaClient(loop=cls.loop, bootstrap_servers=cls.hosts)
            try:
                cls.loop.run_until_complete(client.bootstrap())
                # Broker can still be loading cluster layout, so we can get 0
                # brokers. That counts as still not available
                if client.cluster.brokers():
                    return
            except ConnectionError:
                pass
            finally:
                cls.loop.run_until_complete(client.close())
            time.sleep(0.1)
        assert False, "Kafka server never started"

    @contextmanager
    def silence_loop_exception_handler(self):
        if hasattr(self.loop, "get_exception_handler"):
            orig_handler = self.loop.get_exception_handler()
        else:
            orig_handler = None  # Will set default handler
        self.loop.set_exception_handler(lambda loop, ctx: None)
        yield
        self.loop.set_exception_handler(orig_handler)

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
                      timestamp_ms=None, return_inst=False, headers=None):
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
                    topic, msg, partition=partition,
                    timestamp_ms=timestamp_ms, headers=headers)
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
        self.assertEqual(len(messages), num_messages)

        # Make sure there are no duplicates
        self.assertEqual(len(set(messages)), num_messages)

    def create_ssl_context(self):
        context = create_ssl_context(
            cafile=str(self.ssl_folder / "ca-cert"),
            certfile=str(self.ssl_folder / "cl_client.pem"),
            keyfile=str(self.ssl_folder / "cl_client.key"),
            password="abcdefgh")
        context.check_hostname = False
        return context


def random_string(length):
    s = "".join(random.choice(string.ascii_letters) for _ in range(length))
    return s.encode('utf-8')
