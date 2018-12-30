# import asyncio
from ._testutil import (
    KafkaIntegrationTestCase, run_until_complete, kafka_versions
)

from aiokafka.producer import AIOKafkaProducer
from aiokafka.consumer import AIOKafkaConsumer

# from aiokafka.errors import (
#     UnsupportedVersionError,
#     ProducerFenced, OutOfOrderSequenceNumber, IllegalOperation
# )
# from aiokafka.structs import TopicPartition
# from aiokafka.util import ensure_future


class TestKafkaProducerIntegration(KafkaIntegrationTestCase):

    @kafka_versions('>=0.10.0')
    @run_until_complete
    async def test_sasl_plaintext_basic(self):
        # Produce/consume by SASL_PLAINTEXT
        addr = "{}:{}".format(self.kafka_host, self.kafka_sasl_plain_port)
        producer = AIOKafkaProducer(
            loop=self.loop,
            bootstrap_servers=[addr],
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="test",
            sasl_plain_password="test")
        await producer.start()
        await producer.send_and_wait(topic=self.topic, value=b"Super sasl msg")
        await producer.stop()

        consumer = AIOKafkaConsumer(
            self.topic, loop=self.loop,
            bootstrap_servers=[addr],
            enable_auto_commit=True,
            auto_offset_reset="earliest",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="test",
            sasl_plain_password="test")
        await consumer.start()
        msg = await consumer.getone()
        self.assertEqual(msg.value, b"Super sasl msg")
        await consumer.stop()
