import pytest

from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.errors import (
    GroupAuthorizationFailedError,
    TopicAuthorizationFailedError,
    TransactionalIdAuthorizationFailed,
    UnknownTopicOrPartitionError,
)
from aiokafka.producer import AIOKafkaProducer
from aiokafka.structs import TopicPartition

from ._testutil import KafkaIntegrationTestCase, kafka_versions, run_until_complete


@pytest.mark.usefixtures("setup_test_class")
class TestKafkaSASL(KafkaIntegrationTestCase):
    TEST_TIMEOUT = 60

    # See https://docs.confluent.io/current/kafka/authorization.html
    # for a good list of what Operation can be mapped to what Resource and
    # when is it checked

    def setUp(self):
        super().setUp()
        if tuple(map(int, self.kafka_version.split("."))) >= (0, 10):
            self.acl_manager.list_acl()

    def tearDown(self):
        # This is used to have a better report on ResourceWarnings. Without it
        # all warnings will be filled in the end of last test-case.
        super().tearDown()
        self.acl_manager.cleanup()

    @property
    def sasl_hosts(self):
        # Produce/consume by SASL_PLAINTEXT
        return f"{self.kafka_host}:{self.kafka_sasl_plain_port}"

    @property
    def group_id(self):
        return self.topic + "_group"

    async def producer_factory(self, user="test", **kw):
        producer = AIOKafkaProducer(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kw,
        )
        self.add_cleanup(producer.stop)
        await producer.start()
        return producer

    async def consumer_factory(self, user="test", **kw):
        kwargs = {
            "enable_auto_commit": True,
            "auto_offset_reset": "earliest",
            "group_id": self.group_id,
        }
        kwargs.update(kw)
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kwargs,
        )
        self.add_cleanup(consumer.stop)
        await consumer.start()
        return consumer

    async def admin_client_factory(self, user="test", **kw):
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kw,
        )
        self.add_cleanup(admin_client.close)
        await admin_client.start()
        return admin_client

    async def gssapi_producer_factory(self, **kw):
        producer = AIOKafkaProducer(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="GSSAPI",
            sasl_kerberos_domain_name="localhost",
            **kw,
        )
        self.add_cleanup(producer.stop)
        await producer.start()
        return producer

    async def gssapi_consumer_factory(self, **kw):
        kwargs = {
            "enable_auto_commit": True,
            "auto_offset_reset": "earliest",
            "group_id": self.group_id,
        }
        kwargs.update(kw)
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="GSSAPI",
            sasl_kerberos_domain_name="localhost",
            **kwargs,
        )
        self.add_cleanup(consumer.stop)
        await consumer.start()
        return consumer

    async def gssapi_admin_client_factory(self, **kw):
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="GSSAPI",
            sasl_kerberos_domain_name="localhost",
            **kw,
        )
        self.add_cleanup(admin_client.close)
        await admin_client.start()
        return admin_client

    async def scram_producer_factory(self, user="test", **kw):
        producer = AIOKafkaProducer(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kw,
        )
        self.add_cleanup(producer.stop)
        await producer.start()
        return producer

    async def scram_consumer_factory(self, user="test", **kw):
        kwargs = {
            "enable_auto_commit": True,
            "auto_offset_reset": "earliest",
            "group_id": self.group_id,
        }
        kwargs.update(kw)
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kwargs,
        )
        self.add_cleanup(consumer.stop)
        await consumer.start()
        return consumer

    async def scram_admin_client_factory(self, user="test", **kw):
        admin_client = AIOKafkaAdminClient(
            bootstrap_servers=[self.sasl_hosts],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username=user,
            sasl_plain_password=user,
            **kw,
        )
        self.add_cleanup(admin_client.close)
        await admin_client.start()
        return admin_client

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_sasl_plaintext_basic(self):
        # Produce/consume by SASL_PLAINTEXT
        producer = await self.producer_factory()
        await producer.send_and_wait(topic=self.topic, value=b"Super sasl msg")

        consumer = await self.consumer_factory()
        msg = await consumer.getone()
        self.assertEqual(msg.value, b"Super sasl msg")

    @run_until_complete
    async def test_sasl_plaintext_gssapi(self):
        self.kerberos_utils.kinit("client/localhost")

        try:
            # Produce/consume by SASL_PLAINTEXT
            producer = await self.gssapi_producer_factory()
            await producer.send_and_wait(topic=self.topic, value=b"Super sasl msg")

            consumer = await self.gssapi_consumer_factory()
            msg = await consumer.getone()
            self.assertEqual(msg.value, b"Super sasl msg")
        finally:
            self.kerberos_utils.kdestroy()

    @kafka_versions(">=0.10.2")
    @run_until_complete
    async def test_sasl_plaintext_scram(self):
        self.kafka_config.add_scram_user("test", "test")
        producer = await self.scram_producer_factory()
        await producer.send_and_wait(topic=self.topic, value=b"Super scram msg")

        consumer = await self.scram_consumer_factory()
        msg = await consumer.getone()
        self.assertEqual(msg.value, b"Super scram msg")

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_admin_client_sasl_plaintext_basic(self):
        admin_client = await self.admin_client_factory()
        cluster_info = await admin_client.describe_cluster()
        self.assertGreaterEqual(len(cluster_info["brokers"]), 1)

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_admin_client_sasl_plaintext_gssapi(self):
        self.kerberos_utils.kinit("client/localhost")
        admin_client = await self.gssapi_admin_client_factory()
        cluster_info = await admin_client.describe_cluster()
        self.assertGreaterEqual(len(cluster_info["brokers"]), 1)

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_admin_client_sasl_plaintext_scrum(self):
        self.kafka_config.add_scram_user("test", "test")
        admin_client = await self.scram_admin_client_factory()
        cluster_info = await admin_client.describe_cluster()
        self.assertGreaterEqual(len(cluster_info["brokers"]), 1)

    ##########################################################################
    # Topic Resource
    ##########################################################################

    @kafka_versions(">=0.10.0")
    @kafka_versions("<1.0.0")
    @run_until_complete
    async def test_sasl_unknown_topic_describe(self):
        # Before 1.0.0 Kafka if topic does not exist it will not report
        # Topic authorization errors, so we need to create topic beforehand
        # See https://kafka.apache.org/documentation/#upgrade_100_notable
        tmp_producer = await self.producer_factory()
        await tmp_producer.send_and_wait(self.topic, value=b"Autocreate topic")
        del tmp_producer

        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", topic=self.topic
        )

        producer = await self.producer_factory(request_timeout_ms=10000)

        with self.assertRaises(UnknownTopicOrPartitionError):
            await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        # This will check for authorization on start()
        with self.assertRaises(UnknownTopicOrPartitionError):
            await self.consumer_factory()

    @kafka_versions(">=1.0.0")
    @run_until_complete
    async def test_sasl_deny_topic_describe(self):
        tmp_producer = await self.producer_factory()
        await tmp_producer.send_and_wait(self.topic, value=b"Autocreate topic")
        del tmp_producer

        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", topic=self.topic
        )

        producer = await self.producer_factory(request_timeout_ms=10000)

        with self.assertRaises(TopicAuthorizationFailedError):
            await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        # This will check for authorization on start()
        with self.assertRaises(TopicAuthorizationFailedError):
            await self.consumer_factory()

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_sasl_deny_topic_read(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="READ", topic=self.topic
        )

        producer = await self.producer_factory()
        await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        consumer = await self.consumer_factory()
        with self.assertRaises(TopicAuthorizationFailedError):
            await consumer.getone()

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_sasl_deny_topic_write(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", topic=self.topic
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="WRITE", topic=self.topic
        )

        producer = await self.producer_factory()
        with self.assertRaises(TopicAuthorizationFailedError):
            await producer.send_and_wait(topic=self.topic, value=b"Super sasl msg")

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_autocreate_cluster(self):
        self.acl_manager.add_acl(
            deny_principal="test", operation="CREATE", cluster=True
        )
        self.acl_manager.add_acl(
            allow_principal="test", operation="DESCRIBE", topic=self.topic
        )
        self.acl_manager.add_acl(
            allow_principal="test", operation="WRITE", topic=self.topic
        )

        producer = await self.producer_factory(request_timeout_ms=5000)
        with self.assertRaises(TopicAuthorizationFailedError):
            await producer.send_and_wait(self.topic, value=b"Super sasl msg")

        with self.assertRaises(TopicAuthorizationFailedError):
            await self.consumer_factory(request_timeout_ms=5000)

        with self.assertRaises(TopicAuthorizationFailedError):
            await self.consumer_factory(request_timeout_ms=5000, group_id=None)

    ##########################################################################
    # Group Resource
    ##########################################################################

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_sasl_deny_group_describe(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", group=self.group_id
        )

        # Consumer will require DESCRIBE to perform FindCoordinator
        with self.assertRaises(GroupAuthorizationFailedError):
            consumer = await self.consumer_factory()
            await consumer.getone()

    @kafka_versions(">=0.10.0")
    @run_until_complete
    async def test_sasl_deny_group_read(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="READ", group=self.group_id
        )

        # Consumer will require READ to perform JoinGroup
        with self.assertRaises(GroupAuthorizationFailedError):
            consumer = await self.consumer_factory()
            await consumer.getone()

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_transaction_group_describe(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="DESCRIBE", group=self.group_id
        )

        # Transactional producers will also have the same problem to commit
        producer = await self.producer_factory(transactional_id="test_id")
        with self.assertRaises(GroupAuthorizationFailedError):
            async with producer.transaction():
                await producer.send_offsets_to_transaction(
                    {TopicPartition(self.topic, 0): 0},
                    group_id=self.group_id,
                )

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_transaction_group_read(self):
        self.acl_manager.add_acl(
            allow_principal="test", operation="All", group=self.group_id
        )
        self.acl_manager.add_acl(
            deny_principal="test", operation="READ", group=self.group_id
        )

        # Transactional producers will also have the same problem to commit
        producer = await self.producer_factory(transactional_id="test_id")
        await producer.begin_transaction()
        with self.assertRaises(GroupAuthorizationFailedError):
            await producer.send_offsets_to_transaction(
                {TopicPartition(self.topic, 0): 0},
                group_id=self.group_id,
            )

        with self.assertRaises(GroupAuthorizationFailedError):
            await producer.commit_transaction()
        await producer.abort_transaction()

        # We can continue using producer after this error
        async with producer.transaction():
            await producer.send_and_wait(self.topic, b"TTTT")

    ##########################################################################
    # Transactional ID resource
    ##########################################################################

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_txnid_describe(self):
        self.acl_manager.add_acl(
            allow_principal="test",
            operation="All",
            transactional_id="test_id",
        )
        self.acl_manager.add_acl(
            deny_principal="test",
            operation="DESCRIBE",
            transactional_id="test_id",
        )

        # Transactional producers will require DESCRIBE to perform
        # FindCoordinator
        with self.assertRaises(TransactionalIdAuthorizationFailed):
            await self.producer_factory(transactional_id="test_id")

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_txnid_write(self):
        self.acl_manager.add_acl(
            allow_principal="test",
            operation="All",
            transactional_id="test_id",
        )
        self.acl_manager.add_acl(
            deny_principal="test",
            operation="WRITE",
            transactional_id="test_id",
        )

        # Transactional producers will require DESCRIBE to perform
        # FindCoordinator
        with self.assertRaises(TransactionalIdAuthorizationFailed):
            await self.producer_factory(transactional_id="test_id")

    @kafka_versions(">=0.11.0")
    @run_until_complete
    async def test_sasl_deny_txnid_during_transaction(self):
        self.acl_manager.add_acl(
            allow_principal="test",
            operation="All",
            transactional_id="test_id",
        )

        # Transactional producers will require DESCRIBE to perform
        # FindCoordinator
        producer = await self.producer_factory(transactional_id="test_id")
        await producer.begin_transaction()
        await producer.send_and_wait(self.topic, b"123", partition=0)

        self.acl_manager.add_acl(
            deny_principal="test",
            operation="WRITE",
            transactional_id="test_id",
        )
        with self.assertRaises(TransactionalIdAuthorizationFailed):
            await producer.send_and_wait(self.topic, b"123", partition=1)
