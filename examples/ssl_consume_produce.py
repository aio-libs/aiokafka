import asyncio
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
from aiokafka.structs import TopicPartition

context = create_ssl_context(
    cafile="./ca-cert",  # CA used to sign certificate.
                         # `CARoot` of JKS store container
    certfile="./cert-signed",  # Signed certificate
    keyfile="./cert-key",  # Private Key file of `certfile` certificate
    password="123123"
)

async def produce_and_consume():
    # Produce
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9093',
        security_protocol="SSL", ssl_context=context)

    try:
        await producer.start()
        msg = await producer.send_and_wait(
            'my_topic', b"Super Message", partition=0)
    finally:
        await producer.stop()

    consumer = AIOKafkaConsumer(
        "my_topic", bootstrap_servers='localhost:9093',
        security_protocol="SSL", ssl_context=context)
    try:
        await consumer.start()
        consumer.seek(TopicPartition('my_topic', 0), msg.offset)
        fetch_msg = await consumer.getone()
    finally:
        await consumer.stop()

    print("Success", msg, fetch_msg)

if __name__ == "__main__":
    asyncio.run(produce_and_consume())
