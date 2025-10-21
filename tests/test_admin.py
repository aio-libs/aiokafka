import asyncio

from aiokafka.admin import AIOKafkaAdminClient, NewPartitions, NewTopic, RecordsToDelete
from aiokafka.admin.config_resource import ConfigResource, ConfigResourceType
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer
from aiokafka.structs import TopicPartition

from ._testutil import KafkaIntegrationTestCase, kafka_versions, run_until_complete


class TestAdmin(KafkaIntegrationTestCase):
    async def create_admin(self):
        admin = AIOKafkaAdminClient(bootstrap_servers=self.hosts)
        await admin.start()
        self.add_cleanup(admin.close)
        return admin

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_metadata(self):
        admin = await self.create_admin()
        metadata = await admin._get_cluster_metadata()
        assert metadata.brokers is not None
        assert metadata.topics is not None
        assert len(metadata.brokers) == 1

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_context_manager(self):
        async with AIOKafkaAdminClient(bootstrap_servers=self.hosts) as admin:
            assert admin._started

            # Arbitrary testing
            metadata = await admin._get_cluster_metadata()
            assert metadata.brokers is not None
            assert metadata.topics is not None
            assert len(metadata.brokers) == 1

        assert admin._closed

        # Test error case too
        class FakeError:
            pass

        with pytest.raises(FakeError):
            async with AIOKafkaAdminClient(bootstrap_servers=self.hosts) as admin:
                assert admin._started

                # Arbitrary testing
                metadata = await admin._get_cluster_metadata()
                assert metadata.brokers is not None
                assert metadata.topics is not None
                assert len(metadata.brokers) == 1

                raise FakeError

        assert admin._closed

    @kafka_versions(">=0.10.1.0")
    @run_until_complete
    async def test_create_topics(self):
        admin = await self.create_admin()
        resp = await admin.create_topics([NewTopic(self.topic, 1, 1)])
        assert resp.topic_errors is not None
        assert len(resp.topic_errors) == 1
        topic, error_code, error = resp.topic_errors[0]
        assert topic == self.topic
        assert error_code == 0
        assert not error

    @kafka_versions(">=0.10.1.0")  # Since we use `create_topics()`
    @run_until_complete
    async def test_list_topics(self):
        admin = await self.create_admin()
        topic_names = {self.random_topic_name(), self.random_topic_name()}
        topics = [NewTopic(tn, 1, 1) for tn in topic_names]
        await admin.create_topics(topics)
        actual = await admin.list_topics()
        assert set(actual) >= topic_names

    # @kafka_versions('>=0.10.1.0')
    @kafka_versions(">=1.0.0")  # XXX Times out with 0.10.2.1 and 0.11.0.3
    @run_until_complete
    async def test_delete_topics(self):
        admin = await self.create_admin()
        resp = await admin.create_topics([NewTopic(self.topic, 1, 1)])
        assert resp.topic_errors[0][2] is None
        topics = await admin.list_topics()
        assert self.topic in topics
        resp = await admin.delete_topics([self.topic])
        errors = resp.topic_error_codes
        assert len(errors) == 1
        topic, error_code = errors[0]
        assert topic == self.topic
        assert error_code == 0
        topics = await admin.list_topics()
        assert self.topic not in topics

    @kafka_versions(">=0.11.0.0")
    @run_until_complete
    async def test_describe_configs_topic(self):
        admin = await self.create_admin()
        await admin.create_topics([NewTopic(self.topic, 1, 1)])
        cr = ConfigResource(ConfigResourceType.TOPIC, self.topic)
        resp = await admin.describe_configs([cr])
        assert len(resp) == 1
        assert len(resp[0].resources) == 1
        config_resource = resp[0].resources[0]
        error_code, error_message, resource_type, resource_name, *_ = config_resource
        assert error_code == 0
        assert not error_message  # None or "" depending on kafka version
        assert resource_type == ConfigResourceType.TOPIC
        assert resource_name == self.topic

    @kafka_versions(">=0.11.0.0")
    @run_until_complete
    async def test_describe_configs_broker(self):
        admin = await self.create_admin()
        [broker_id] = admin._client.cluster._brokers.keys()
        cr = ConfigResource(ConfigResourceType.BROKER, broker_id)
        resp = await admin.describe_configs([cr])
        assert len(resp) == 1
        assert len(resp[0].resources) == 1
        config_resource = resp[0].resources[0]
        error_code, error_message, resource_type, resource_name, *_ = config_resource
        assert error_code == 0
        assert not error_message  # None or "" depending on kafka version
        assert resource_type == ConfigResourceType.BROKER
        assert resource_name == str(broker_id)

    @kafka_versions(">=0.11.0.0")
    @run_until_complete
    async def test_alter_configs(self):
        admin = await self.create_admin()
        await admin.create_topics([NewTopic(self.topic, 1, 1)])
        cr = ConfigResource(
            ConfigResourceType.TOPIC, self.topic, {"cleanup.policy": "delete"}
        )
        await admin.alter_configs([cr])
        new_configs_resp = await admin.describe_configs([cr])
        assert len(new_configs_resp) == 1
        assert len(new_configs_resp[0].resources) == 1
        config_entries = new_configs_resp[0].resources[0][4]
        assert len(config_entries) == 1
        name, value, *_ = config_entries[0]
        assert name == "cleanup.policy"
        assert value == "delete"

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_describe_cluster(self):
        admin = await self.create_admin()
        [broker_id] = admin._client.cluster._brokers.keys()
        resp = await admin.describe_cluster()
        assert len(resp["brokers"]) == 1
        assert resp["brokers"][0]["node_id"] == broker_id

    @kafka_versions(">=1.0.0")
    @run_until_complete
    async def test_create_partitions(self):
        admin = await self.create_admin()
        await admin.create_topics([NewTopic(self.topic, 1, 1)])
        old_desc = await admin.describe_topics([self.topic])
        old_partitions = {p["partition"] for p in old_desc[0]["partitions"]}
        assert len(old_partitions) == 1

        new_part = NewPartitions(total_count=2)
        await admin.create_partitions({self.topic: new_part})
        new_desc = await admin.describe_topics([self.topic])
        new_partitions = {p["partition"] for p in new_desc[0]["partitions"]}
        assert len(new_partitions) == 2
        assert new_partitions > old_partitions

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_list_consumer_groups(self):
        admin = await self.create_admin()
        group_id = f"group-{self.id()}"
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group_id, bootstrap_servers=self.hosts
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)
        await asyncio.sleep(0.1)  # Otherwise we can get GroupLoadInProgressError

        resp = await admin.list_consumer_groups()
        assert len(resp) >= 1  # There can be group left from other test
        groups = [group for group, *_ in resp]
        assert group_id in groups

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_describe_consumer_groups(self):
        admin = await self.create_admin()
        group_id = f"group-{self.id()}"
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group_id, bootstrap_servers=self.hosts
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        resp = await admin.describe_consumer_groups([group_id])
        assert len(resp) == 1
        assert len(resp[0].groups) == 1
        error_code, group, *_ = resp[0].groups[0]
        assert error_code == 0
        assert group == group_id

    @kafka_versions(">=0.10.0.0")
    @run_until_complete
    async def test_list_consumer_group_offsets(self):
        admin = await self.create_admin()
        group_id = f"group-{self.id()}"

        consumer = AIOKafkaConsumer(
            self.topic,
            group_id=group_id,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        async with AIOKafkaProducer(bootstrap_servers=self.hosts) as producer:
            await producer.send_and_wait(self.topic, b"some-message")
            await producer.send_and_wait(self.topic, b"other-message")

        msg = await consumer.getone()
        await consumer.commit()
        resp = await admin.list_consumer_group_offsets(group_id)
        tp = TopicPartition(msg.topic, msg.partition)
        assert resp[tp].offset == msg.offset + 1
        resp = await admin.list_consumer_group_offsets(group_id, partitions=[tp])
        assert resp[tp].offset == msg.offset + 1

    @kafka_versions(">=1.1.0")
    @run_until_complete
    async def test_delete_records(self):
        admin = await self.create_admin()

        await admin.create_topics([NewTopic(self.topic, 1, 1)])

        async with AIOKafkaProducer(bootstrap_servers=self.hosts) as producer:
            first_message = await producer.send_and_wait(
                self.topic, partition=0, value=b"some-message"
            )
            await producer.send_and_wait(
                self.topic, partition=0, value=b"other-message"
            )

        await admin.delete_records(
            {
                TopicPartition(self.topic, 0): RecordsToDelete(
                    before_offset=first_message.offset + 1
                )
            }
        )

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        msg = await consumer.getone()
        assert msg.value == b"other-message"
