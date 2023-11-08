Serialization and compression
=============================

Kafka supports several compression types: ``gzip``, ``snappy`` and ``lz4``. You only
need to specify the compression in Kafka Producer, Consumer will decompress
automatically.

Note:
    Messages are compressed in batches, so you will have more efficiency on
    larger batches. You can consider setting `linger_ms` to batch more data
    before sending.

By default :attr:`~aiokafka.structs.ConsumerRecord.value` and
:attr:`~aiokafka.structs.ConsumerRecord.key` attributes of returned
:class:`~aiokafka.structs.ConsumerRecord` instances are :class:`bytes`. You can
use custom serializer/deserializer hooks to operate on objects instead of
:class:`bytes` in those attributes.

Producer

.. code:: python

    import json
    import asyncio
    from aiokafka import AIOKafkaProducer

    def serializer(value):
        return json.dumps(value).encode()

    async def produce():
        producer = AIOKafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=serializer,
            compression_type="gzip")

        await producer.start()
        data = {"a": 123.4, "b": "some string"}
        await producer.send('foobar', data)
        data = [1,2,3,4]
        await producer.send('foobar', data)
        await producer.stop()

    asyncio.run(produce())


Consumer

.. code:: python

    import json
    import asyncio
    from aiokafka.errors import KafkaError
    from aiokafka import AIOKafkaConsumer

    def deserializer(serialized):
        return json.loads(serialized)

    async def consume():
        # consumer will decompress messages automatically
        # in accordance to compression type specified in producer
        consumer = AIOKafkaConsumer(
            'foobar',
            bootstrap_servers='localhost:9092',
            value_deserializer=deserializer,
            auto_offset_reset='earliest')
        await consumer.start()
        data = await consumer.getmany(timeout_ms=10000)
        for tp, messages in data.items():
            for message in messages:
                print(type(message.value), message.value)
        await consumer.stop()

    asyncio.run(consume())

Output:

  >>> python3 producer.py
  >>> python3 consumer.py
  <class 'dict'> {'a': 123.4, 'b': 'some string'}
  <class 'list'> [1,2,3,4]
