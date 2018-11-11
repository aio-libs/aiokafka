from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

from aiokafka.producer import AIOKafkaProducer
from aiokafka.consumer import AIOKafkaConsumer

from aiokafka.errors import (
    UnsupportedVersionError,
    ProducerFenced, OutOfOrderSequenceNumber
)
from aiokafka.structs import TopicPartition


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('<0.11.0')
    @run_until_complete
    async def test_producer_transactions_not_supported(self):
        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        producer
        with self.assertRaises(UnsupportedVersionError):
            await producer.start()
        await producer.stop()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_simple(self):
        # The test here will just check if we can do simple produce with
        # transactional_id option and minimal setup.

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        async with producer.transaction():
            meta = await producer.send_and_wait(
                self.topic, b'hello, Kafka!')

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            auto_offset_reset="earliest")
        await consumer.start()
        self.add_cleanup(consumer.stop)
        msg = await consumer.getone()
        self.assertEqual(msg.offset, meta.offset)
        self.assertEqual(msg.timestamp, meta.timestamp)
        self.assertEqual(msg.value, b"hello, Kafka!")
        self.assertEqual(msg.key, None)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_empty_txn(self):
        # If we commit or abort transaction that was never started we should
        # not even send the End marker

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer")
        await producer.start()
        self.add_cleanup(producer.stop)

        await producer.begin_transaction()
        await producer.commit_transaction()

        await producer.begin_transaction()
        await producer.abort_transaction()

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_fences_off_previous(self):
        # Test 2 producers fencing one another by using the same
        # transactional_id

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p2")
        await producer2.start()
        self.add_cleanup(producer2.stop)
        async with producer2.transaction():
            await producer2.send_and_wait(self.topic, b'hello, Kafka! 2')

        with self.assertRaises(ProducerFenced):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'hello, Kafka!')

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_restart_reaquire_pid(self):
        # While it's documented that PID may change we need to be sure we
        # are sending proper InitPIDRequest, not an indempotent one

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        pid = producer._txn_manager.producer_id
        await producer.stop()

        producer2 = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p2")
        await producer2.start()
        self.add_cleanup(producer2.stop)
        self.assertEqual(pid, producer2._txn_manager.producer_id)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_raise_out_of_sequence(self):
        # If we were to fail to send some message we should get\
        # OutOfOrderSequenceNumber

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(OutOfOrderSequenceNumber):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'msg1', partition=0)
                # Imitate a not delivered message
                producer._txn_manager.increment_sequence_number(
                    TopicPartition(self.topic, 0), 1)
                await producer.send_and_wait(self.topic, b'msg2', partition=0)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_aborting_previous_failure(self):
        # If we were to fail to send some message we should get\
        # OutOfOrderSequenceNumber

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        with self.assertRaises(OutOfOrderSequenceNumber):
            async with producer.transaction():
                await producer.send_and_wait(self.topic, b'msg1', partition=0)
                # Imitate a not delivered message
                producer._txn_manager.increment_sequence_number(
                    TopicPartition(self.topic, 0), 1)
                await producer.send_and_wait(self.topic, b'msg2', partition=0)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_send_offsets_to_transaction(self):
        # This is a pair test of Consume - To - Produce processing. We consume
        # a batch, process, produce with Procuder and send commit through
        # Producer also. At the end commit the transaction through Producer.
        # This will update commit point in Consumer too.

        # Setup some messages in INPUT topic
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        in_topic = self.topic
        out_topic = self.topic + "-out"
        group_id = self.topic + "-group"

        consumer = AIOKafkaConsumer(
            in_topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            group_id=group_id,
            auto_offset_reset="earliest")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        assignment = consumer.assignment()
        self.assertTrue(assignment)
        for tp in assignment:
            await consumer.commit({tp: 0})
            offset_before = await consumer.committed(tp)
            self.assertEqual(offset_before, 0)

        async def transform():
            while True:
                batch = await consumer.getmany(timeout_ms=5000, max_records=20)
                if not batch:
                    break
                async with producer.transaction():
                    offsets = {}
                    for tp, msgs in batch.items():
                        for msg in msgs:
                            out_msg = b"OUT-" + msg.value
                            # We produce to the same partition
                            producer.send(
                                out_topic, value=out_msg,
                                partition=tp.partition)
                        offsets[tp] = msg.offset + 1
                    await producer.send_offsets_to_transaction(
                        offsets, group_id)

        await transform()
        for tp in assignment:
            offset = await consumer.committed(tp)
            self.assertEqual(offset, 100)

    @kafka_versions('>=0.11.0')
    @run_until_complete
    async def test_producer_transactional_send_offsets_and_abort(self):
        # Following previous, we will process but abort transaction. Commit
        # should not be processed and the same data should be returned after
        # reset

        # Setup some messages in INPUT topic
        await self.send_messages(0, list(range(0, 100)))
        await self.send_messages(1, list(range(100, 200)))
        in_topic = self.topic
        out_topic = self.topic + "-out"
        group_id = self.topic + "-group"

        consumer = AIOKafkaConsumer(
            in_topic, loop=self.loop,
            bootstrap_servers=self.hosts,
            enable_auto_commit=False,
            group_id=group_id,
            auto_offset_reset="earliest")
        await consumer.start()
        self.add_cleanup(consumer.stop)

        producer = AIOKafkaProducer(
            loop=self.loop, bootstrap_servers=self.hosts,
            transactional_id="sobaka_producer", client_id="p1")
        await producer.start()
        self.add_cleanup(producer.stop)

        assignment = consumer.assignment()
        self.assertTrue(assignment)
        for tp in assignment:
            await consumer.commit({tp: 0})
            offset_before = await consumer.committed(tp)
            self.assertEqual(offset_before, 0)

        async def transform(raise_error):
            while True:
                batch = await consumer.getmany(timeout_ms=5000, max_records=20)
                if not batch:
                    break
                async with producer.transaction():
                    offsets = {}
                    for tp, msgs in batch.items():
                        for msg in msgs:
                            out_msg = b"OUT-" + msg.value
                            # We produce to the same partition
                            producer.send(
                                out_topic, value=out_msg,
                                partition=tp.partition)
                        offsets[tp] = msg.offset + 1
                    await producer.send_offsets_to_transaction(
                        offsets, group_id)
                    if raise_error:
                        raise ValueError()

        try:
            await transform(raise_error=True)
        except ValueError:
            pass

        for tp in assignment:
            offset = await consumer.committed(tp)
            self.assertEqual(offset, 0)

        await consumer.seek_to_committed()
        await transform(raise_error=False)

        for tp in assignment:
            offset = await consumer.committed(tp)
            self.assertEqual(offset, 100)
