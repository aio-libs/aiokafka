import asyncio
import random

from aiokafka.producer import AIOKafkaProducer

loop = asyncio.get_event_loop()


async def send_many(num):
    topic  = "my_topic"
    producer = AIOKafkaProducer(loop=loop)
    await producer.start()

    batch = producer.create_batch()

    i = 0
    while i < num:
        payload = "Test message %d" % i
        if not batch.append(key=None, value=payload.encode(), timestamp=None):
            partitions = await producer.partitions_for(topic)
            partition = random.choice(tuple(partitions))
            await producer.send_batch(batch, topic, partition)
            batch = producer.create_batch()
            continue
        i += 1
    partitions = await producer.partitions_for(topic)
    partition = random.choice(tuple(partitions))
    await producer.send_batch(batch, topic, partition)
    await producer.stop()


loop.run_until_complete(send_many(100))
loop.close()
