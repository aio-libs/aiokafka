from aiokafka import AIOKafkaProducer
import asyncio

loop = asyncio.get_event_loop()


async def send_one():
    producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        transactional_id="transactional_test",
        loop=loop)

    # Get cluster layout and topic/partition allocation
    await producer.start()
    try:
        async with producer.transaction():
            # Produce messages
            res = await producer.send_and_wait(
                "test-topic", b"Super transactional message")
            input()
            raise ValueError()
    finally:
        await producer.stop()
    print(res)

loop.run_until_complete(send_one())
