Manual commit
=============

When processing sensitive data, using ``enable_auto_commit=True`` (default) for the
Consumer can lead to data loss in the event of a critical failure. To avoid
this, set ``enable_auto_commit=False`` and commit offsets manually only after
messages have been processed. Note, that this is a tradeoff from *at most once*
to *at least once* delivery, to achieve *exactly once* you will need to save
offsets in the destination database and validate those yourself.

More on message delivery: https://kafka.apache.org/documentation.html#semantics

.. note::
    After Kafka Broker version 0.11 and after `aiokafka==0.5.0` it is possible
    to use Transactional Producer to achieve *exactly once* delivery semantics.
    See :ref:`transactional-producer` section.


Consumer:

.. code:: python

    import json
    import asyncio
    from aiokafka.errors import KafkaError
    from aiokafka import AIOKafkaConsumer

    async def consume():
        consumer = AIOKafkaConsumer(
            'foobar',
            bootstrap_servers='localhost:9092',
            auto_offset_reset='earliest',
            group_id="some-consumer-group",
            enable_auto_commit=False)
        await consumer.start()
        # we want to consume 10 messages from "foobar" topic
        # and commit after that
        for _ in range(10):
            msg = await consumer.getone()
        await consumer.commit()

        await consumer.stop()

    asyncio.run(consume())
