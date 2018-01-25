aiokafka
========
.. image:: https://travis-ci.org/aio-libs/aiokafka.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiokafka
    :alt: |Build status|
.. image:: https://codecov.io/github/aio-libs/aiokafka/coverage.svg?branch=master
    :target: https://codecov.io/gh/aio-libs/aiokafka/branch/master
    :alt: |Coverage|
.. image:: https://badges.gitter.im/Join%20Chat.svg
    :target: https://gitter.im/aio-libs/Lobby
    :alt: |Chat on Gitter|

asyncio client for Kafka


AIOKafkaProducer
****************

AIOKafkaProducer is a high-level, asynchronous message producer.

Example of AIOKafkaProducer usage:

.. code-block:: python

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


AIOKafkaConsumer
****************

AIOKafkaConsumer is a high-level, asynchronous message consumer.
It interacts with the assigned Kafka Group Coordinator node to allow multiple 
consumers to load balance consumption of topics (requires kafka >= 0.9.0.0).

Example of AIOKafkaConsumer usage:

.. code-block:: python

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

Running tests
-------------

Docker is required to run tests. See https://docs.docker.com/engine/installation for installation notes. Also note, that `lz4` compression libraries for python will require `python-dev` package,
or python source header files for compilation on Linux.

Setting up tests requirements (assuming you're within virtualenv on ubuntu 14.04+)::

    sudo apt-get install -y libsnappy-dev
    make setup

Running tests::

    make cov

To run tests with a specific version of Kafka (default one is 0.10.1.0) use KAFKA_VERSION variable::

    make cov KAFKA_VERSION=0.10.0.0

Test running cheatsheat:

 * ``make test FLAGS="-l -x --ff"`` - run until 1 failure, rerun failed tests fitst. Great for cleaning up a lot of errors, say after a big refactor.
 * ``make test FLAGS="-k consumer"`` - run only the consumer tests.
 * ``make test FLAGS="-m 'not ssl'"`` - run tests excluding ssl.
 * ``make test FLAGS="--no-pull"`` - do not try to pull new docker image before test run.

