
Example. Manual commit
======================

When processing data from several


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

