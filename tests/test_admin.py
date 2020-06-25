from aiokafka.admin import AIOKafkaAdminClient
from ._testutil import KafkaIntegrationTestCase, run_until_complete

from kafka.admin import NewTopic
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

    @property
    def topic_name(self):
        return f"topic-{hash(random.random())}"
