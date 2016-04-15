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
