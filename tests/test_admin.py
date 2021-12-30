from kafka.admin import NewTopic, NewPartitions
from kafka.admin.config_resource import ConfigResource, ConfigResourceType

from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.consumer import AIOKafkaConsumer
from ._testutil import (
    KafkaIntegrationTestCase, kafka_versions, run_until_complete
)


class TestAdmin(KafkaIntegrationTestCase):
    async def create_admin(self):
        admin = AIOKafkaAdminClient(bootstrap_servers=self.hosts)
        await admin.start()
        self.add_cleanup(admin.close)
        return admin

    @kafka_versions('>=0.10.0.0')
    @run_until_complete
    async def test_metadata(self):
        admin = await self.create_admin()
        metadata = await admin._get_cluster_metadata()
        assert metadata.brokers is not None
        assert metadata.topics is not None
        assert len(metadata.brokers) == 1

    @kafka_versions('>=0.10.1.0')
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

    @kafka_versions('>=0.10.1.0')  # Since we use `create_topics()`
    @run_until_complete
    async def test_list_topics(self):
        admin = await self.create_admin()
        topic_names = {self.random_topic_name(), self.random_topic_name()}
        topics = [NewTopic(tn, 1, 1) for tn in topic_names]
        await admin.create_topics(topics)
        actual = await admin.list_topics()
        assert set(actual) >= topic_names

    # @kafka_versions('>=0.10.1.0')
    @kafka_versions('>=1.0.0')  # XXX Timeouts with 0.10.2.1 and 0.11.0.3
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

    @kafka_versions('>=0.11.0.0')
    @run_until_complete
    async def test_describe_configs(self):
        admin = await self.create_admin()
        await admin.create_topics([NewTopic(self.topic, 1, 1)])
        cr = ConfigResource(ConfigResourceType.TOPIC, self.topic, None)
        resp = await admin.describe_configs([cr])
        assert len(resp) == 1
        assert len(resp[0].resources) == 1
        config_resource = resp[0].resources[0]
        error_code, error_message, resource_type, resource_name, *_ = config_resource
        assert error_code == 0
        assert not error_message  # None or "" depending on kafka version
        assert resource_type == ConfigResourceType.TOPIC
        assert resource_name == self.topic

    @kafka_versions('>=0.11.0.0')
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

    @kafka_versions('>=1.0.0')
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

    @kafka_versions('>=0.10.0.0')
    @run_until_complete
    async def test_list_consumer_groups(self):
        admin = await self.create_admin()
        resp = await admin.list_consumer_groups()
        assert len(resp) == 0

        group_id = f'group-{self.id()}'
        consumer = AIOKafkaConsumer(
            self.topic, group_id=group_id, bootstrap_servers=self.hosts
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        resp = await admin.list_consumer_groups()
        assert len(resp) == 1
        actual_group, protocol_type, *_ = resp[0]
        assert actual_group == group_id
        assert protocol_type == 'consumer'
