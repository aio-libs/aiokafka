
Custom partitioner
==================

If you consider using partitions as a logical entity, rather then purely for
load-balancing, you may need to have more control over routing messages to
partitions. By default hashing algorithms are used.


Producer

.. code:: python

        import asyncio
        import random
        from aiokafka import AIOKafkaProducer

        def my_partitioner(key, all_partitions, available_partitions):
           if key == b'first':
               return all_partitions[0]
           elif key == b'last':
               return all_partitions[-1]
           return random.choice(all_partitions)

        @asyncio.coroutine
        def produce_one(producer, key, value):
            future = yield from producer.send('foobar', value, key=key)
            resp = yield from future
            print("'%s' produced in partition: %i"%(value.decode(), resp.partition))

        @asyncio.coroutine
        def produce_task(loop):
            producer = AIOKafkaProducer(
                loop=loop, bootstrap_servers='localhost:9092',
                partitioner=my_partitioner)

            yield from producer.start()
            yield from produce_one(producer, b'last', b'1')
            yield from produce_one(producer, b'some', b'2')
            yield from produce_one(producer, b'first', b'3')
            yield from producer.stop()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(produce_task(loop))
        loop.close()



Output (topic `foobar` has 10 partitions):

>>> python3 producer.py
'1' produced in partition: 9
'2' produced in partition: 6
'3' produced in partition: 0

