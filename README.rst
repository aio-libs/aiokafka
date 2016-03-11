aiokafka
========
.. image:: https://travis-ci.org/aio-libs/aiokafka.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiokafka
    :alt: |Build status|
.. image:: https://coveralls.io/repos/aio-libs/aiokafka/badge.png?branch=master
    :target: https://coveralls.io/r/aio-libs/aiokafka?branch=master
    :alt: |Coverage|

asyncio client for kafka


AIOKafkaProducer
****************

AIOKafkaProducer is a high-level, asynchronous message producer.

Example of AIOKafkaProducer usage:

.. code-block:: python

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


AIOKafkaConsumer
****************

AIOKafkaConsumer is a high-level, asynchronous message consumer.
It also interacts with the assigned kafka Group Coordinator nodeto allow multiple consumers to load balance consumption of topics (requires kafka >= 0.9.0.0).

Example of AIOKafkaConsumer usage:

.. code-block:: python

        import asyncio
        from kafka.common import KafkaError
        from aiokafka import AIOKafkaConsumer

        @asyncio.coroutine
        def consume_task(consumer):
            while True:
                try:
                    msg = yield from consumer.get_message()
                    print("consumed: ", msg.topic, msg.partition, msg.offset, msg.value)
                except KafkaError as err:
                    print("error while consuming message: ", err)

        loop = asyncio.get_event_loop()
        consumer = AIOKafkaConsumer('topic1', 'topic2', loop=loop, bootstrap_servers='localhost:1234')
        loop.run_until_complete(consumer.start())
        asyncio.async(consume_task(consumer))
        loop.run_until_complete(asyncio.sleep(5))
        loop.run_until_complete(consumer.stop())
        c_task.close()
        loop.close()


Running tests
-------------

Docker is required to run tests. See https://docs.docker.com/engine/installation for installation notes.

Setting up tests requirements (assuming you're within virtualenv on ubuntu 14.04+)::

    sudo apt-get install -y libsnappy-dev
    pip install flake8 pytest pytest-cov pytest-catchlog docker-py python-snappy coveralls .

Running tests::

    make cov

To run tests with a specific version of Kafka (default one is 0.9.0.1) use KAFKA_VERSION variable::

    make cov KAFKA_VERSION=0.8.2.1

