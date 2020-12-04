import asyncio
import random

from aiokafka.producer import AIOKafkaProducer


async def send_many(num):
    topic  = "my_topic"
    producer = AIOKafkaProducer()
    await producer.start()

    batch = producer.create_batch()

    i = 0
    while i < num:
        msg = ("Test message %d" % i).encode("utf-8")
        metadata = batch.append(key=None, value=msg, timestamp=None)
        if metadata is None:
            partitions = await producer.partitions_for(topic)
            partition = random.choice(tuple(partitions))
            await producer.send_batch(batch, topic, partition=partition)
            print("%d messages sent to partition %d"
                  % (batch.record_count(), partition))
            batch = producer.create_batch()
            continue
        i += 1
    partitions = await producer.partitions_for(topic)
    partition = random.choice(tuple(partitions))
    await producer.send_batch(batch, topic, partition=partition)
    print("%d messages sent to partition %d"
          % (batch.record_count(), partition))
    await producer.stop()


asyncio.run(send_many(1000))
