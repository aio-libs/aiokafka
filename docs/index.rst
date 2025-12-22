Welcome to aiokafka's documentation!
====================================

.. _GitHub: https://github.com/aio-libs/aiokafka
.. _asyncio: https://docs.python.org/3/library/asyncio.html
.. _kafka-python: https://github.com/dpkp/kafka-python

.. image:: https://img.shields.io/pypi/pyversions/aiokafka.svg
    :target: https://pypi.python.org/pypi/aiokafka
.. image:: https://img.shields.io/badge/license-Apache%202-blue.svg
    :target: https://github.com/aio-libs/aiokafka/blob/master/LICENSE

**aiokafka** is a client for the Apache Kafka distributed stream processing system using asyncio_.
It is based on the kafka-python_ library and reuses its internals for protocol parsing, errors, etc.
The client is designed to function much like the official Java client, with a sprinkling of Pythonic interfaces.

**aiokafka** can be used with 0.11+ Kafka brokers and supports fully coordinated
consumer groups -- i.e., dynamic partition assignment to multiple consumers in
the same group.


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

    async def consume():
        consumer = AIOKafkaConsumer(
            'my_topic', 'my_other_topic',
            bootstrap_servers='localhost:9092',
            group_id="my-group")
        try:
            # Get cluster layout and join group `my-group`
            await consumer.start()
            # Consume messages
            async for msg in consumer:
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                      msg.key, msg.value, msg.timestamp)
        finally:
            # Will leave consumer group; perform autocommit if enabled.
            await consumer.stop()

    asyncio.run(consume())

Read more in :ref:`Consumer client <consumer-usage>` section.

AIOKafkaProducer
++++++++++++++++

:class:`~aiokafka.AIOKafkaProducer` is a high-level, asynchronous message producer.

Here's a producer example:

.. code:: python

    from aiokafka import AIOKafkaProducer
    import asyncio

    async def send_one():
        producer = AIOKafkaProducer(
            bootstrap_servers='localhost:9092')
        try:
            # Get cluster layout and initial topic/partition leadership information
            await producer.start()
            # Produce message
            await producer.send_and_wait("my_topic", b"Super message")
        finally:
            # Wait for all pending messages to be delivered or expire.
            await producer.stop()

    asyncio.run(send_one())

Read more in :ref:`Producer client <producer-usage>` section.

Installation
------------

.. code:: bash

    pip install aiokafka


Optional LZ4 install
++++++++++++++++++++

To enable LZ4 compression/decompression, install **aiokafka** with :code:`lz4` extra option:

.. code:: bash

    pip install 'aiokafka[lz4]'

Note, that on **Windows** you will need Visual Studio build tools, available for download
from http://landinghub.visualstudio.com/visual-cpp-build-tools


Optional Snappy install
+++++++++++++++++++++++

To enable Snappy compression/decompression, install **aiokafka** with :code:`snappy` extra option

.. code:: bash

    pip install 'aiokafka[snappy]'


Optional zstd indtall
+++++++++++++++++++++

To enable Zstandard compression/decompression, install **aiokafka** with :code:`zstd` extra option:

.. code:: bash

    pip install 'aiokafka[zstd]'


Optional GSSAPI install
+++++++++++++++++++++++

To enable SASL authentication with GSSAPI, install **aiokafka** with :code:`gssapi` extra option:

.. code:: bash

    pip install 'aiokafka[gssapi]'


Source code
-----------

The project is hosted on GitHub_

Please feel free to file an issue on `bug tracker
<https://github.com/aio-libs/aiokafka/issues>`_ if you have found a bug
or have some suggestion for library improvement.


Authors and License
-------------------

The **aiokafka** package is Apache 2 licensed and freely available.

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
