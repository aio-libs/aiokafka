
Python 3.5 async usage
======================

``aiokafka`` supports Python3.5 ``async def`` syntax and adds some sugar using
this syntax.


**You can use AIOKafkaConsumer as a simple async iterator**

.. note::
    All not-critical errors will just be logged to `aiokafka` logger when using
    async iterator. See `Errors handling` of :ref:`API Documentation <api-doc>` section.


.. code:: python

    from aiokafka import AIOKafkaConsumer
    import asyncio

    loop = asyncio.get_event_loop()

    async def consume():
        consumer = AIOKafkaConsumer(
            "my_topic", loop=loop, bootstrap_servers='localhost:9092')
        # Get cluster layout and topic/partition allocation
        await consumer.start()
        try:
            async for msg in consumer:
                print(msg.value)
        finally:
            await consumer.stop()

    loop.run_until_complete(consume())
