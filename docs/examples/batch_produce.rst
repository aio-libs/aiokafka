
Batch producer
==============

If your application needs precise control over batch creation and submission
and you're willing to forego the niceties of automatic serialization and
partition selection, you  may use the simple ``create_batch()`` and
``send_batch()`` interface.


Producer

.. code:: python

    import asyncio
    import random
    from aiokafka.producer import AIOKafkaProducer

    async def send_many(num, loop):
        topic  = "my_topic"
        producer = AIOKafkaProducer(loop=loop)
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

    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_many(1000, loop))
    loop.close()


Output (topic `my_topic` has 3 partitions):

>>> python3 batch_produce.py
329 messages sent to partition 2
327 messages sent to partition 0
327 messages sent to partition 0
17 messages sent to partition 1
