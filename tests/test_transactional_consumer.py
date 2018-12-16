import asyncio

from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

from aiokafka.producer import AIOKafkaProducer
from aiokafka.consumer import AIOKafkaConsumer
from aiokafka.structs import TopicPartition

from aiokafka.errors import (
    UnsupportedVersionError,
)


class TestKafkaConsumerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('<0.11.0')
    @run_until_complete
    async def test_consumer_transactions_not_supported(self):
        consumer = AIOKafkaConsumer(
            loop=self.loop, bootstrap_servers=self.hosts,
            isolation_level="read_committed")
        with self.assertRaises(UnsupportedVersionError):
            await consumer.start()
        await consumer.stop()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_transactional_commit(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        await producer2.start()
        self.add_cleanup(producer2.stop)

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
            isolation_level="read_committed")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        # We will produce from a transactional producer and then from a
        # non-transactional. This should block consumption on that partition
        # until transaction is committed.
        await producer.begin_transaction()
        meta = await producer.send_and_wait(
            self.topic, b'Hello from transaction', partition=0)

        meta2 = await producer2.send_and_wait(
            self.topic, b'Hello from non-transaction', partition=0)

        # The transaction blocked consumption
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(1, loop=self.loop)
        self.assertFalse(task.done())

        tp = TopicPartition(self.topic, 0)
        self.assertEqual(consumer.last_stable_offset(tp), 0)
        self.assertEqual(consumer.highwater(tp), 2)

        await producer.commit_transaction()

        # Order should be preserved. We first yield the first message, although
        # it belongs to a committed afterwards transaction
        msg = await task
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"Hello from transaction")
        self.assertEqual(msg.key, None)

        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta2.offset)
        self.assertEqual(msg.timestamp, meta2.timestamp)
        self.assertEqual(msg.value, b"Hello from non-transaction")
        self.assertEqual(msg.key, None)

        # 3, because we have a commit marker also
        tp = TopicPartition(self.topic, 0)
        self.assertEqual(consumer.last_stable_offset(tp), 3)
        self.assertEqual(consumer.highwater(tp), 3)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_transactional_abort(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts)
        await producer2.start()
        self.add_cleanup(producer2.stop)

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
            isolation_level="read_committed")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        # We will produce from a transactional producer and then from a
        # non-transactional. This should block consumption on that partition
        # until transaction is committed.
        await producer.begin_transaction()
        await producer.send_and_wait(
            self.topic, b'Hello from transaction', partition=0)

        meta2 = await producer2.send_and_wait(
            self.topic, b'Hello from non-transaction', partition=0)

        # The transaction blocked consumption
        task = self.loop.create_task(consumer.getone())
        await asyncio.sleep(1, loop=self.loop)
        self.assertFalse(task.done())

        tp = TopicPartition(self.topic, 0)
        self.assertEqual(consumer.last_stable_offset(tp), 0)
        self.assertEqual(consumer.highwater(tp), 2)

        await producer.abort_transaction()

        # Order should be preserved. We first yield the first message, although
        # it belongs to a committed afterwards transaction
        msg = await task
        self.assertEqual(msg.offset, meta2.offset)
        self.assertEqual(msg.timestamp, meta2.timestamp)
        self.assertEqual(msg.value, b"Hello from non-transaction")
        self.assertEqual(msg.key, None)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(
                consumer.getone(), timeout=0.5, loop=self.loop)

        tp = TopicPartition(self.topic, 0)
        self.assertEqual(consumer.last_stable_offset(tp), 3)
        self.assertEqual(consumer.highwater(tp), 3)

    async def _test_control_record(self, isolation_level):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        async with producer.transaction():
            meta = await producer.send_and_wait(
                self.topic, b'Hello from transaction', partition=0)

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
            isolation_level=isolation_level,
            fetch_max_bytes=10)
        await consumer.start()
        self.add_cleanup(consumer.stop)

        # Transaction marker will be next after the message
        consumer.seek(meta.topic_partition, meta.offset + 1)

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(
                consumer.getone(), timeout=0.5, loop=self.loop)

        # We must not be stuck on previous position
        position = await consumer.position(meta.topic_partition)
        self.assertEqual(position, meta.offset + 2)

        # After producing some more data it should resume consumption
        async with producer.transaction():
            meta2 = await producer.send_and_wait(
                self.topic, b'Hello from transaction 2', partition=0)

        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta2.offset)
        self.assertEqual(msg.timestamp, meta2.timestamp)
        self.assertEqual(msg.value, b"Hello from transaction 2")
        self.assertEqual(msg.key, None)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_transactional_read_only_control_record(self):
        await self._test_control_record("read_committed")

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_simple_read_only_control_record(self):
        await self._test_control_record("read_uncommitted")

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_consumer_several_transactions(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        msgs = []
        for i in range(10):
            await producer.begin_transaction()
            msg = b'Hello ' + str(i).encode()
            await producer.send(self.topic, msg, partition=0)
            if i % 3 == 0:
                await producer.commit_transaction()
                msgs.append(msg)
            else:
                await producer.abort_transaction()

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest",
            isolation_level="read_committed")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        async for msg in consumer:
            self.assertEqual(msg.value, msgs.pop(0))
            if not msgs:
                break

        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(
                consumer.getone(), timeout=0.5, loop=self.loop)
