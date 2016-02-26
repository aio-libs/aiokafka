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

```python

import asyncio
from aiokafka import AIOKafkaProducer

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

```
