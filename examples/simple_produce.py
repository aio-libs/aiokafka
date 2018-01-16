from aiokafka import AIOKafkaProducer
import asyncio

loop = asyncio.get_event_loop()

async def send_one():
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092')
    # Get cluster layout and topic/partition allocation
    await producer.start()
    while True:
        try:
            # Produce messages
            res = await producer.send_and_wait("test-topic", b"Super message")
            print(res)
            await asyncio.sleep(1)
        except:
            await producer.stop()
            raise

loop.run_until_complete(send_one())
