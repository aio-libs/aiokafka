
Manual commit
=============

When processing more sensitive data ``enable_auto_commit=False`` mode of
Consumer can lead to data loss in cases of critical failure. To avoid it we
can commit offsets manually after they were processed. Note, that this is a
tradeoff from *at most once* to *at least once* delivery, to achieve
*exactly once* you will need to save offsets in the destination database and
validate those yourself.

More on message delivery: https://kafka.apache.org/documentation.html#semantics

.. note::
    After Kafka Broker version 0.11 and after `aiokafka==0.5.0` it is possible
    to use Transactional Producer to achive *exactly once* delivery semantics.
    See :ref:`Tranactional Producer <transactional-producer>` section.


Consumer:

.. code:: python

    import json
    import asyncio
    from kafka.common import KafkaError
    from aiokafka import AIOKafkaConsumer

    loop = asyncio.get_event_loop()
    consumer = AIOKafkaConsumer(
        'foobar', loop=loop,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id="some-consumer-group",
        enable_auto_commit=False)
    loop.run_until_complete(consumer.start())
    # we want to consume 10 messages from "foobar" topic
    # and commit after that
    for i in range(10):
        msg = loop.run_until_complete(consumer.getone())
    loop.run_until_complete(consumer.commit())

    loop.run_until_complete(consumer.stop())
    loop.close()

