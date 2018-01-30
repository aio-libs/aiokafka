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

**aiokafka** can be used with 0.9+ Kafka brokers and supports fully coordinated
consumer groups -- i.e., dynamic partition assignment to multiple consumers in
the same group. For now new features of Kafka 11+ are yet to be implemented,
including Transactional Producer.


Getting started
---------------


AIOKafkaConsumer
++++++++++++++++

:class:`~aiokafka.AIOKafkaConsumer` is a high-level message consumer, intended to
operate as similarly as possible to the official Java client.

Here's a consumer example:

.. code:: python

    from aiokafka import AIOKafkaConsumer
    import asyncio

    loop = asyncio.get_event_loop()

    async def consume():
        consumer = AIOKafkaConsumer(
            'my_topic', 'my_other_topic',
            loop=loop, bootstrap_servers='localhost:9092',
            group_id="my-group")
        # Get cluster layout and join group `my-group`
        await consumer.start()
        try:
            # Consume messages
            async for msg in consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                      msg.key, msg.value, msg.timestamp)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

    loop.run_until_complete(consume())

Read more in :ref:`Consumer client <consumer-usage>` section.

AIOKafkaProducer
++++++++++++++++

:class:`~aiokafka.AIOKafkaProducer` is a high-level, asynchronous message producer.

Here's a producer example:

.. code:: python

    from aiokafka import AIOKafkaProducer
    import asyncio

    loop = asyncio.get_event_loop()

    async def send_one():
        producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers='localhost:9092')
        # Get cluster layout and initial topic/partition leadership information
        await producer.start()
        try:
            # Produce message
            await producer.send_and_wait("my_topic", b"Super message")
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

    loop.run_until_complete(send_one())

Read more in :ref:`Producer client <producer-usage>` section.

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

Note, that on **Windows** you will need Visual Studio build tools, available for download
from http://landinghub.visualstudio.com/visual-cpp-build-tools


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

For **Windows** the easiest way is to fetch a precompiled wheel from
http://www.lfd.uci.edu/~gohlke/pythonlibs/#python-snappy



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

   producer
   consumer
   kafka-python_difference
   api
   examples


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
