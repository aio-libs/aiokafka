from aiokafka.consumer import AIOKafkaConsumer
from ._testutil import KafkaIntegrationTestCase, run_until_complete


class TestConsumerIteratorIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    async def test_aiter(self):
        await self.send_messages(0, list(range(0, 10)))
        await self.send_messages(1, list(range(10, 20)))

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset='earliest')
        await consumer.start()

        messages = []
        async for m in consumer:
            messages.append(m)
            if len(messages) == 20:
                break
        self.assert_message_count(messages, 20)
        await consumer.stop()
