
Group consumer
==============

As of Kafka 9.0 Consumers can consume on the same topic simultaneously. This
is achieved by coordinating consumers by one of Kafka broker nodes
(coordinator). This node will perform synchronization of partition assignment
(thou the partitions will be assigned by python code) and consumers will always
return messages for the assigned partitions.

Note:
    Though Consumer will never return messages from not assigned partitions,
    if you are in autocommit=False mode, you should re-check assignment
    before processing next message returned by `getmany()` call.


Producer:

.. code:: python

        import sys
        import asyncio
        from aiokafka import AIOKafkaProducer

        @asyncio.coroutine
        def produce(loop, value, partition):
            producer = AIOKafkaProducer(
                loop=loop, bootstrap_servers='localhost:9092')

            yield from producer.start()
            yield from producer.send('some-topic', value, partition=partition)
            yield from producer.stop()

        if len(sys.argv) != 3:
            print("usage: producer.py <partition> <message>")
            sys.exit(1)
        value = sys.argv[2].encode()
        partition = int(sys.argv[1])

        loop = asyncio.get_event_loop()
        loop.run_until_complete(produce(loop, value, partition))
        loop.close()


Consumer:

.. code:: python
 
        import sys
        import asyncio
        from aiokafka import AIOKafkaConsumer

        if len(sys.argv) < 3:
            print("usage: consumer.py <group_id> <wait messages count>")
            sys.exit(1)
        group_id = sys.argv[1]
        msg_cnt = int(sys.argv[2])

        loop = asyncio.get_event_loop()
        consumer = AIOKafkaConsumer(
            'some-topic', loop=loop,
            group_id=group_id,
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest')
        loop.run_until_complete(consumer.start())
        for _ in range(msg_cnt):
            msg = loop.run_until_complete(consumer.getone())
            print("Message from partition [{}]: {}".format(msg.partition, msg.value))
        loop.run_until_complete(consumer.stop())
        loop.close()



Run example scripts:

creating topic "some-topic" with 2 partitions using standard Kafka utility:

>>> bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic some-topic

terminal#1:

>>> python3 consumer.py TEST_GROUP 2

terminal#2:

>>> python3 consumer.py TEST_GROUP 2

terminal#3:

>>> python3 consumer.py OTHER_GROUP 4

terminal#4:

>>> python3 producer.py 0 'message #1'
>>> python3 producer.py 0 'message #2'
>>> python3 producer.py 1 'message #3'
>>> python3 producer.py 1 'message #4'


Output:

terminal#1:

Message from partition [0]: b'message #1'

Message from partition [0]: b'message #2'

terminal#2:

Message from partition [1]: b'message #3'

Message from partition [1]: b'message #4'

terminal#3:

Message from partition [1]: b'message #3'

Message from partition [1]: b'message #4'

Message from partition [0]: b'message #1'

Message from partition [0]: b'message #2'
