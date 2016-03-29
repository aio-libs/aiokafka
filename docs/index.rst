
Welcome to aiokafka's documentation!
====================================

.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

**aiokafka** is a client for the Apache Kafka distributed stream processing system using the asyncio_.
Client is designed to function much like the official java client, with a sprinkling of pythonic interfaces.

**aiokafka** is used with 0.9 Kafka brokers and supports fully coordinated consumer groups -- i.e., dynamic
partition assignment to multiple consumers in the same group.


Installation
------------

.. code::

   pip3 install aiokafka

.. note:: *aiokafka* requires *python-kafka* library.


Getting started
---------------

:class:`~aiokafka.AIOKafkaConsumer` is a high-level message consumer, intended to
operate as similarly as possible to the official 0.9 java client. Full support
for coordinated consumer groups requires use of kafka brokers that support the
0.9 Group APIs.
See `AIOKafkaConsumer <apidoc/aiokafka.consumer.html>`_ for more details.

See consumer example:

.. code:: python

        import asyncio
        from kafka.common import KafkaError
        from aiokafka import AIOKafkaConsumer

        @asyncio.coroutine
        def consume_task(consumer):
            while True:
                try:
                    msg = yield from consumer.getone()
                    print("consumed: ", msg.topic, msg.partition, msg.offset, msg.value)
                except KafkaError as err:
                    print("error while consuming message: ", err)

        loop = asyncio.get_event_loop()
        consumer = AIOKafkaConsumer('topic1', 'topic2', loop=loop, bootstrap_servers='localhost:1234')
        loop.run_until_complete(consumer.start())
        c_task = asyncio.async(consume_task(consumer))
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(consumer.stop())
            c_task.close()
            loop.close()

:class:`~aiokafka.AIOKafkaProducer` is a high-level, asynchronous message producer.
The class is intended to operate as similarly as possible to the official java client.
See `AIOKafkaProducer <apidoc/aiokafka.producer.html>`_ for more details.

See producer example:

.. code:: python

        import asyncio
        from aiokafka import AIOKafkaProducer

        @asyncio.coroutine
        def produce(loop):
            producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:1234')
            yield from producer.start()
            future1 = producer.send('foobar', b'some_message_bytes')
            future2 = producer.send('foobar', key=b'foo', value=b'bar')
            future3 = producer.send('foobar', b'message for partition 1', partition=1)
            yield from asyncio.wait([future1, future2, future3], loop=loop)
            yield from producer.stop()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(produce(loop))
        loop.close()


Compression
-----------

aiokafka supports gzip compression/decompression natively. To produce or
consume lz4 compressed messages, you must install lz4tools and xxhash.
To enable snappy, install python-snappy (also requires snappy library).

API Documentation
-----------------

        * `Producer API <apidoc/aiokafka.producer.html>`_
        * `Consumer API <apidoc/aiokafka.consumer.html>`_

Examples
--------

`Serialization and compression <serialize_and_compress.html>`_

`Manual commit <manual_commit.html>`_

`Group consumer <group_consumer.html>`_
