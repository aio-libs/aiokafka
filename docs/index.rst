Welcome to aiokafka's documentation!
====================================

.. _GitHub: https://github.com/aio-libs/aiokafka
.. _kafka-python: https://github.com/dpkp/kafka-python
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

**aiokafka** is a client for the Apache Kafka distributed stream processing system using the asyncio_.
It is based on kafka-python_ library and reuses it's internals for protocol parsing, errors, etc. 
Client is designed to function much like the official java client, with a sprinkling of pythonic interfaces.

**aiokafka** is used with 0.9 Kafka brokers and supports fully coordinated consumer groups -- i.e., dynamic
partition assignment to multiple consumers in the same group.


Getting started
---------------


AIOKafkaConsumer
++++++++++++++++

:class:`~aiokafka.AIOKafkaConsumer` is a high-level message consumer, intended to
operate as similarly as possible to the official 0.9 java client. Full support
for coordinated consumer groups requires use of kafka brokers that support the
0.9 Group APIs.

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
        consumer = AIOKafkaConsumer(
            'topic1', 'topic2', loop=loop, bootstrap_servers='localhost:1234')
        loop.run_until_complete(consumer.start())
        c_task = asyncio.async(consume_task(consumer))
        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(consumer.stop())
            c_task.close()
            loop.close()

AIOKafkaProducer
++++++++++++++++

:class:`~aiokafka.AIOKafkaProducer` is a high-level, asynchronous message producer.
The class is intended to operate as similarly as possible to the official java client.

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


Installation
------------

.. code::

   pip3 install aiokafka

.. note:: *aiokafka* requires *python-kafka* library and heavily depands on it.


Optional LZ4 install
++++++++++++++++++++

To enable LZ4 compression/decompression, install lz4tools and xxhash:

>>> pip3 install lz4tools
>>> pip3 install xxhash


Optional Snappy install
+++++++++++++++++++++++

1. Download and build Snappy from http://code.google.com/p/snappy/downloads/list

Ubuntu:

.. code:: bash

    apt-get install libsnappy-dev

OSX:

.. code:: bash

    brew install snappy

From Source:

.. code:: bash

    wget http://snappy.googlecode.com/files/snappy-1.0.5.tar.gz
    tar xzvf snappy-1.0.5.tar.gz
    cd snappy-1.0.5
    ./configure
    make
    sudo make install


2. Install the `python-snappy` module

.. code:: bash

    pip3 install python-snappy



Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aiokafka/issues>`_ if you have found a bug
or have some suggestion for library improvement.

The library uses `Travis <https://travis-ci.org/aio-libs/aiokafka>`_ for
Continious Integration.


Authors and License
-------------------

The ``aiokafka`` package is Apache 2 licensed and freely available.

Feel free to improve this package and send a pull request to GitHub_.


Contents:

.. toctree::
   :maxdepth: 2

   api
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
