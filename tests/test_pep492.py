import asyncio

from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.errors import ConsumerStoppedError, NoOffsetForPartitionError
from aiokafka.util import create_task

from ._testutil import KafkaIntegrationTestCase, run_until_complete


class TestConsumerIteratorIntegration(KafkaIntegrationTestCase):
    @run_until_complete
    async def test_aiter(self):
        await self.send_messages(0, list(range(10)))
        await self.send_messages(1, list(range(10, 20)))

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        messages = []
        async for m in consumer:
            messages.append(m)
            if len(messages) == 20:
                break

        self.assert_message_count(messages, 20)

    @run_until_complete
    async def test_exception_in_aiter(self):
        await self.send_messages(0, [b"test"])

        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="none",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        with self.assertRaises(NoOffsetForPartitionError):
            async for _m in consumer:
                pass  # pragma: no cover

    @run_until_complete
    async def test_consumer_stops_iter(self):
        consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self.add_cleanup(consumer.stop)

        async def iterator():
            async for msg in consumer:  # pragma: no cover
                raise AssertionError(f"No items should be here, got {msg}")

        task = create_task(iterator())
        await asyncio.sleep(0.1)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        await consumer.stop()
        # Should just stop iterator, no errors
        await task
        # But creating another iterator should result in an error, we can't
        # have dead loops like:
        #
        #   while True:
        #     async for msg in consumer:
        #       print(msg)
        with self.assertRaises(ConsumerStoppedError):
            await iterator()
