import asyncio
import os

from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def main() -> None:
    client = AIOKafkaAdminClient(bootstrap_servers=os.environ["BOOTSTRAP_SERVERS"])
    await client.start()
    try:
        for i in range(20):
            topic = f"test-{i}"
            print("Creating topic:", topic)
            await client.create_topics(
                [NewTopic(name=topic, num_partitions=3, replication_factor=2)]
            )
            await asyncio.sleep(1)
            print("Deleting topic:", topic)
            await client.delete_topics([topic])
            await asyncio.sleep(1)
    finally:
        await client.close()


if __name__ == "__main__":
    # Start the asyncio loop by running the main function
    asyncio.run(main())
