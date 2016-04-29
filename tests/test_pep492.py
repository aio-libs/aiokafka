from aiokafka.consumer import AIOKafkaConsumer
from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, random_string)


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

    @run_until_complete
    async def test_exception_ignored_with_aiter(self):
        l_msgs = [random_string(10), random_string(50000)]
        large_messages = await self.send_messages(0, l_msgs)
        r_msgs = [random_string(50)]
        small_messages = await self.send_messages(0, r_msgs)

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset='earliest',
            max_partition_fetch_bytes=4000)
        await consumer.start()

        messages = []
        with self.assertLogs('aiokafka.consumer', level='ERROR') as cm:
            async for m in consumer:
                messages.append(m)
                if len(messages) == 2:
                    break

            self.assertEqual(len(cm.output), 1)
            self.assertTrue(
                'ERROR:aiokafka.consumer:error in consumer iterator'
                in cm.output[0])
        self.assertEqual(messages[0].value, large_messages[0])
        self.assertEqual(messages[1].value, small_messages[0])
        await consumer.stop()
