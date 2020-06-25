from aiokafka.admin import AIOKafkaAdminClient
from ._testutil import KafkaIntegrationTestCase, run_until_complete

from kafka.admin import NewTopic, NewPartitions
from kafka.admin.config_resource import ConfigResource, ConfigResourceType
import random


class TestAdmin(KafkaIntegrationTestCase):
    def setUp(self):
        super().setUp()
        self.admin = AIOKafkaAdminClient(
            bootstrap_servers=self.hosts,
            loop=self.loop,
            api_version="1.0.0")
        self.loop.run_until_complete(self.admin.start())

    def tearDown(self):
        self.loop.run_until_complete(self.admin.close())
        super().tearDown()

    @run_until_complete
    async def test_metadata(self):
        metadata = await self.admin._get_cluster_metadata()
        assert metadata.brokers is not None
        assert metadata.topics is not None
        assert len(metadata.brokers) == 1

    @run_until_complete
    async def test_create_topics(self):
        name = self.topic_name
        resp = await self.admin.create_topics([NewTopic(name, 1, 1)])
        assert resp.topic_errors is not None
        assert len(resp.topic_errors) == 1
        topic, error_code, error = resp.topic_errors[0]
        assert topic == name
        assert error_code == 0
        assert not error

    @run_until_complete
    async def test_list_topics(self):
        topic_names = {self.topic_name, self.topic_name}
        topics = [NewTopic(tn, 1, 1) for tn in topic_names]
        await self.admin.create_topics(topics)
        actual = await self.admin.list_topics()
        for tn in topic_names:
            assert tn in actual

    @run_until_complete
    async def test_delete_topics(self):
        name = self.topic_name
        resp = await self.admin.create_topics([NewTopic(name, 1, 1)])
        assert resp.topic_errors[0][2] is None
        topics = await self.admin.list_topics()
        assert name in topics
        resp = await self.admin.delete_topics([name])
        errors = resp.topic_error_codes
        assert len(errors) == 1
        topic, error_code = errors[0]
        assert topic == name
        assert error_code == 0
        topics = await self.admin.list_topics()
        assert name not in topics

    @run_until_complete
    async def test_describe_configs(self):
        tn = self.topic_name
        await self.admin.create_topics([NewTopic(tn, 1, 1)])
        cr = ConfigResource(ConfigResourceType.TOPIC, tn, None)
        resp = await self.admin.describe_configs([cr])
        assert len(resp) == 1
        assert len(resp[0].resources) == 1
        config_resource = resp[0].resources[0]
        assert config_resource[3] == tn
        assert False, config_resource

    @run_until_complete
    async def test_alter_configs(self):
        tn = self.topic_name
        await self.admin.create_topics([NewTopic(tn, 1, 1)])
        cr = ConfigResource(ConfigResourceType.TOPIC, tn, {"cleanup.policy": "delete"})
        await self.admin.alter_configs([cr])
        new_configs_resp = await self.admin.describe_configs([cr])
        assert len(new_configs_resp) == 1
        assert len(new_configs_resp[0].resources) == 1
        name, value, _, _, _, _ = new_configs_resp[0].resources[0][4][0]
        assert name == "cleanup.policy"
        assert value == "delete"

    @run_until_complete
    async def test_create_partitions(self):
        tn = self.topic_name
        await self.admin.create_topics([NewTopic(tn, 1, 1)])
        old_desc = await self.admin.describe_topics([tn])
        assert len(old_desc[0]["partitions"]) == 1
        new_part = NewPartitions(total_count=2)
        await self.admin.create_partitions({tn: new_part})
        new_desc = await self.admin.describe_topics([tn])
        assert len(new_desc[0]["partitions"]) == 2
        assert new_desc[0]["partitions"][0] == old_desc[0]["partitions"][0]

    @run_until_complete
    async def test_list_consumer_groups(self):
        resp = await self.admin.list_consumer_groups()
        assert len(resp) == 0

    @property
    def topic_name(self):
        return f"topic-{hash(random.random())}"
