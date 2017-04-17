Welcome to aiokafka's documentation!
====================================

.. _GitHub: https://github.com/aio-libs/aiokafka
.. _kafka-python: https://github.com/dpkp/kafka-python
.. _asyncio: http://docs.python.org/3.4/library/asyncio.html

.. image:: https://img.shields.io/badge/kafka-0.10%2C%200.9-brightgreen.svg
    :target: https://kafka.apache.org
.. image:: https://img.shields.io/pypi/pyversions/aiokafka.svg
    :target: https://pypi.python.org/pypi/aiokafka
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://github.com/aio-libs/aiokafka/blob/master/LICENSE

**aiokafka** is a client for the Apache Kafka distributed stream processing system using asyncio_.
It is based on the kafka-python_ library and reuses its internals for protocol parsing, errors, etc.
The client is designed to function much like the official Java client, with a sprinkling of Pythonic interfaces.

**aiokafka** is used with 0.9/0.10 Kafka brokers and supports fully coordinated consumer groups -- i.e., dynamic
partition assignment to multiple consumers in the same group.


Getting started
---------------


AIOKafkaConsumer
++++++++++++++++

:class:`~aiokafka.AIOKafkaConsumer` is a high-level message consumer, intended to
operate as similarly as possible to the official Java client.

Here's a consumer example:

.. code:: python

    import asyncio
    from kafka.common import KafkaError
    from aiokafka import AIOKafkaConsumer

    @asyncio.coroutine
    def consume_task(consumer):
        while True:
            try:
                msg = yield from consumer.getone()
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                      msg.key, msg.value, msg.timestamp)
            except KafkaError as err:
                print("error while consuming message: ", err)

    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(
        'topic1', 'topic2', loop=loop, bootstrap_servers='localhost:1234')
    # Bootstrap client, will get initial cluster metadata
    loop.run_until_complete(consumer.start())
    c_task = loop.create_task(consume_task(consumer))
    try:
        loop.run_forever()
    finally:
        # Will gracefully leave consumer group; perform autocommit if enabled
        loop.run_until_complete(consumer.stop())
        c_task.cancel()
        loop.close()


AIOKafkaProducer
++++++++++++++++

:class:`~aiokafka.AIOKafkaProducer` is a high-level, asynchronous message producer.
The class is intended to operate as similarly as possible to the official Java client.

Here's a producer example:

.. code:: python

    import asyncio
    from aiokafka import AIOKafkaProducer

    @asyncio.coroutine
    def produce(loop):
        # Just adds message to sending queue
        future = yield from producer.send('foobar', b'some_message_bytes')
        # waiting for message to be delivered
        resp = yield from future
        print("Message produced: partition {}; offset {}".format(
              resp.partition, resp.offset))
        # Also can use a helper to send and wait in 1 call
        resp = yield from producer.send_and_wait(
            'foobar', key=b'foo', value=b'bar')
        resp = yield from producer.send_and_wait(
            'foobar', b'message for partition 1', partition=1)

    loop = asyncio.get_event_loop()
    producer = AIOKafkaProducer(loop=loop, bootstrap_servers='localhost:9092')
    # Bootstrap client, will get initial cluster metadata
    loop.run_until_complete(producer.start())
    loop.run_until_complete(produce(loop))
    # Wait for all pending messages to be delivered or expire
    loop.run_until_complete(producer.stop())
    loop.close()


Installation
------------

.. code::

   pip3 install aiokafka

.. note:: *aiokafka* requires the *kafka-python* library.


Optional LZ4 install
++++++++++++++++++++

To enable LZ4 compression/decompression, install lz4tools and xxhash:

>>> pip3 install lz4tools
>>> pip3 install xxhash


Optional Snappy install
+++++++++++++++++++++++

1. Download and build Snappy from http://google.github.io/snappy/

Ubuntu:

.. code:: bash

    apt-get install libsnappy-dev

OSX:

.. code:: bash

    brew install snappy

From Source:

.. code:: bash

    wget https://github.com/google/snappy/tarball/master
    tar xzvf google-snappy-X.X.X-X-XXXXXXXX.tar.gz
    cd google-snappy-X.X.X-X-XXXXXXXX
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
   :maxdepth: 3

   consumer
   kafka-python_difference
   api
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
