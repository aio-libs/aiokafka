from aiokafka import AIOKafkaProducer
import asyncio

async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        transactional_id="transactional_test")

    # Get cluster layout and topic/partition allocation
    try:
        await producer.start()
        async with producer.transaction():
            # Produce messages
            res = await producer.send_and_wait(
                "test-topic", b"Super transactional message")
            input()
            raise ValueError()
    finally:
        await producer.stop()
    print(res)

asyncio.run(send_one())
