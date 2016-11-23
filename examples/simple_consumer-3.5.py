import asyncio

from aiokafka import AIOKafkaConsumer

import logging

log_level = logging.DEBUG
log_format = '[%(asctime)s] %(levelname)s [%(name)s]: %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
# log = logging.getLogger('kafka')
# log.setLevel(log_level)

loop = asyncio.get_event_loop()

async def consume():
    consumer = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092',
        metadata_max_age_ms=5000, group_id="test2")
    consumer.subscribe(pattern="test*")
    # Get cluster layout and topic/partition allocation
    await consumer.start()
    try:
        async for msg in consumer:
            print(msg.value)
    finally:
        await consumer.stop()

loop.run_until_complete(consume())
