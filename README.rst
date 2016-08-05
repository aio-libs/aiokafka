aiokafka
========
.. image:: https://travis-ci.org/aio-libs/aiokafka.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiokafka
    :alt: |Build status|
.. image:: https://coveralls.io/repos/aio-libs/aiokafka/badge.png?branch=master
    :target: https://coveralls.io/r/aio-libs/aiokafka?branch=master
    :alt: |Coverage|

asyncio client for Kafka


AIOKafkaProducer
****************

AIOKafkaProducer is a high-level, asynchronous message producer.

Example of AIOKafkaProducer usage:

.. code-block:: python

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
    loop.run_until_complete(producer.start())
    loop.run_until_complete(produce(loop))
    loop.run_until_complete(producer.stop())
    loop.close()


AIOKafkaConsumer
****************

AIOKafkaConsumer is a high-level, asynchronous message consumer.
It interacts with the assigned Kafka Group Coordinator node to allow multiple consumers to load balance consumption of topics (requires kafka >= 0.9.0.0).

Example of AIOKafkaConsumer usage:

.. code-block:: python

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
    c_task = loop.create_task(consume_task(consumer))
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(consumer.stop())
        c_task.cancel()
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
