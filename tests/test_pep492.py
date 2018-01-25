import asyncio
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.errors import ConsumerStoppedError, NoOffsetForPartitionError
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
        self.add_cleanup(consumer.stop)

        messages = []
        async for m in consumer:
            messages.append(m)
            if len(messages) == 20:
                # Flake8==3.0.3 gives
                #   F999 'break' outside loop
                # for `async` syntax
                break  # noqa

        self.assert_message_count(messages, 20)

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
        self.add_cleanup(consumer.stop)

        messages = []
        with self.assertLogs(
                'aiokafka.consumer.consumer', level='ERROR') as cm:
            async for m in consumer:
                messages.append(m)
                if len(messages) == 2:
                    # Flake8==3.0.3 gives
                    #   F999 'break' outside loop
                    # for `async` syntax
                    break  # noqa

            self.assertEqual(len(cm.output), 1)
            self.assertTrue(
                'ERROR:aiokafka.consumer.consumer:error in consumer iterator'
                in cm.output[0])
        self.assertEqual(messages[0].value, large_messages[0])
        self.assertEqual(messages[1].value, small_messages[0])

    @run_until_complete
    async def test_exception_in_aiter(self):
        await self.send_messages(0, [b'test'])

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="none")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        with self.assertRaises(NoOffsetForPartitionError):
            async for m in consumer:
                m  # pragma: no cover

    @run_until_complete
    async def test_consumer_stops_iter(self):
        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        async def iterator():
            async for msg in consumer:  # pragma: no cover
                assert False, "No items should be here, got {}".format(msg)

        task = self.loop.create_task(iterator())
        await asyncio.sleep(0.1, loop=self.loop)
        # As we didn't input any data into Kafka
        self.assertFalse(task.done())

        await consumer.stop()
        # Should just stop iterator, no errors
        await task
        # But creating anothe iterator should result in an error, we can't
        # have dead loops like:
        #
        #   while True:
        #     async for msg in consumer:
        #       print(msg)
        with self.assertRaises(ConsumerStoppedError):
            await iterator()
