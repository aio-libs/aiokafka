# import asyncio
from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

from aiokafka.producer import AIOKafkaProducer
from aiokafka.consumer import AIOKafkaConsumer

from aiokafka.errors import (
    TopicAuthorizationFailedError, GroupAuthorizationFailedError
)
from aiokafka.structs import TopicPartition


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

    # See https://docs.confluent.io/current/kafka/authorization.html
    # for a good list of what Operation can be mapped to what Resource and
    # when is it checked

    @property
    def sasl_hosts(self):
        # Produce/consume by SASL_PLAINTEXT
        return "{}:{}".format(self.kafka_host, self.kafka_sasl_plain_port)

    @property
    def group_id(self):
        return self.topic + "_group"

    async def producer_factory(self, user="test", **kw):
        producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kw)
        self.add_cleanup(producer.stop)
        await producer.start()
        return producer

    async def consumer_factory(self, user="test", **kw):
        kwargs = dict(
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            group_id=self.group_id
        )
        kwargs.update(kw)
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kwargs)
        self.add_cleanup(consumer.stop)
        await consumer.start()
        return consumer

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_plaintext_basic(self):
        # Produce/consume by SASL_PLAINTEXT
        producer = await self.producer_factory()
        await producer.send_and_wait(topic=self.topic, value=b"Super sasl msg")

        consumer = await self.consumer_factory()
        msg = await consumer.getone()
        self.assertEqual(msg.value, b"Super sasl msg")

    ##########################################################################
    # Topic Resource
    ##########################################################################

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_deny_topic_describe(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic)
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", topic=self.topic)

        producer = await self.producer_factory()
        with self.assertRaises(TopicAuthorizationFailedError):
            await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        # This will check for authorization on start()
        with self.assertRaises(TopicAuthorizationFailedError):
            await self.consumer_factory()

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_deny_topic_read(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic)
        self.acl_manager.add_acl(
            deny_principal="test", operation="READ", topic=self.topic)

        producer = await self.producer_factory()
        await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        consumer = await self.consumer_factory()
        with self.assertRaises(TopicAuthorizationFailedError):
            await consumer.getone()

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_deny_topic_write(self):

        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic)
        self.acl_manager.add_acl(
            deny_principal="test", operation="WRITE", topic=self.topic)

        producer = await self.producer_factory()
        with self.assertRaises(TopicAuthorizationFailedError):
            await producer.send_and_wait(
                topic=self.topic, value=b"Super sasl msg")

    ##########################################################################
    # Group Resource
    ##########################################################################

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_deny_group_describe(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id)
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", group=self.group_id)

        # Consumer will require DESCRIBE to perform FindCoordinator
        with self.assertRaises(GroupAuthorizationFailedError):
            consumer = await self.consumer_factory()
            await consumer.getone()

        # Transactional producers will also have the same problem to commit
        producer = await self.producer_factory(transactional_id="test_id")
        with self.assertRaises(GroupAuthorizationFailedError):
            async with producer.transaction():
                await producer.send_offsets_to_transaction(
                    {TopicPartition(self.topic, 0): 0},
                    group_id=self.group_id)

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_deny_group_read(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id)
        self.acl_manager.add_acl(
            deny_principal="test", operation="READ", group=self.group_id)

        # Consumer will require READ to perform JoinGroup
        with self.assertRaises(GroupAuthorizationFailedError):
            consumer = await self.consumer_factory()
            await consumer.getone()

        # Transactional producers will also have the same problem to commit
        producer = await self.producer_factory(transactional_id="test_id")
        with self.assertRaises(GroupAuthorizationFailedError):
            async with producer.transaction():
                await producer.send_offsets_to_transaction(
                    {TopicPartition(self.topic, 0): 0},
                    group_id=self.group_id)
