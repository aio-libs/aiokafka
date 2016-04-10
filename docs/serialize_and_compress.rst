
Example. Serialization and compression
======================================

Producer:

.. code:: python

        import json
        import asyncio
        from aiokafka import AIOKafkaProducer

        def serializer(value):
            return json.dumps(value).encode()

        @asyncio.coroutine
        def produce(loop):
            producer = AIOKafkaProducer(
                loop=loop, bootstrap_servers='localhost:9092',
                value_serializer=serializer,
                compression_type="gzip")

            yield from producer.start()
            data = {"a": 123.4, "b": "some string"}
            yield from producer.send('foobar', data)
            data = [1,2,3,4]
            yield from producer.send('foobar', data)
            yield from producer.stop()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(produce(loop))
        loop.close()


Consumer:

.. code:: python
 
        import json
        import asyncio
        from kafka.common import KafkaError
        from aiokafka import AIOKafkaConsumer

        def deserializer(serialized):
            return json.loads(serialized)

        loop = asyncio.get_event_loop()
        # consumer will decompress messages automatically
        # in accordance to compression type specified in producer
        consumer = AIOKafkaConsumer(
            'foobar', loop=loop,
            bootstrap_servers='localhost:9092',
            value_deserializer=deserializer,
            auto_offset_reset='earliest')
        loop.run_until_complete(consumer.start())
        data = loop.run_until_complete(consumer.getmany(timeout_ms=10000))
        for tp, messages in data.items():
            for message in messages:
                print(type(message.value), message.value)
        loop.run_until_complete(consumer.stop())
        loop.close()


Output:

>>> python3 producer.py
>>> python3 consumer.py
<class 'dict'> {'a': 123.4, 'b': 'some string'}
<class 'list'> [1,2,3,4]

