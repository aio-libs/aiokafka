Group consumer
==============

As of Kafka 9.0 Consumers can consume on the same topic simultaneously. This
is achieved by coordinating consumers by one of Kafka broker nodes
(coordinator). This node will perform synchronization of partition assignment
(thou the partitions will be assigned by python code) and consumers will always
return messages for the assigned partitions.

Note:
    Though Consumer will never return messages from not assigned partitions,
    if you are in ``autocommit=False`` mode, you should re-check assignment
    before processing next message returned by
    :meth:`~.AIOKafkaConsumer.getmany` call.


Producer:

.. code:: python

        import sys
        import asyncio
        from aiokafka import AIOKafkaProducer

        async def produce(value, partition):
            producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')

            await producer.start()
            await producer.send('some-topic', value, partition=partition)
            await producer.stop()

        if len(sys.argv) != 3:
            print("usage: producer.py <partition> <message>")
            sys.exit(1)
        value = sys.argv[2].encode()
        partition = int(sys.argv[1])

        asyncio.run(produce(value, partition))


Consumer:

.. code:: python

        import sys
        import asyncio
        from aiokafka import AIOKafkaConsumer

        async def consume():
            consumer = AIOKafkaConsumer(
                'some-topic',
                group_id=group_id,
                bootstrap_servers='localhost:9092',
                auto_offset_reset='earliest')
            await consumer.start()
            for _ in range(msg_cnt):
                msg = await consumer.getone()
                print(f"Message from partition [{msg.partition}]: {msg.value}")
            await consumer.stop()

        if len(sys.argv) < 3:
            print("usage: consumer.py <group_id> <wait messages count>")
            sys.exit(1)
        group_id = sys.argv[1]
        msg_cnt = int(sys.argv[2])

        asyncio.run(consume(group_id, msg_cnt))



Run example scripts:

* Creating topic ``some-topic`` with 2 partitions using standard Kafka utility::

    bin/kafka-topics.sh --create \
      --zookeeper localhost:2181 \
      --replication-factor 1 \
      --partitions 2 \
      --topic some-topic

* terminal#1::

    python3 consumer.py TEST_GROUP 2

* terminal#2::

    python3 consumer.py TEST_GROUP 2

* terminal#3::

    python3 consumer.py OTHER_GROUP 4

* terminal#4::

    python3 producer.py 0 'message #1'
    python3 producer.py 0 'message #2'
    python3 producer.py 1 'message #3'
    python3 producer.py 1 'message #4'


Output:

* terminal#1::

    Message from partition [0]: b'message #1'

    Message from partition [0]: b'message #2'

* terminal#2::

    Message from partition [1]: b'message #3'

    Message from partition [1]: b'message #4'

* terminal#3::

    Message from partition [1]: b'message #3'

    Message from partition [1]: b'message #4'

    Message from partition [0]: b'message #1'

    Message from partition [0]: b'message #2'
