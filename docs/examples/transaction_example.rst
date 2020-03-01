.. _transaction-example:

Transactional Consume-Process-Produce
-------------------------------------

If you have a pattern where you want to consume from one topic, process data
and produce to a different one, you would really like to do it with using
Transactional Producer. In the example below we read from ``IN_TOPIC``,
process data and produce the resut to ``OUT_TOPIC`` in a transactional manner.


.. code:: python

    import asyncio
    from collections import defaultdict, Counter

    from aiokafka import TopicPartition, AIOKafkaConsumer, AIOKafkaProducer


    IN_TOPIC = "in_topic"
    GROUP_ID = "processing-group"
    OUT_TOPIC = "out_topic"
    TRANSACTIONAL_ID = "my-txn-id"
    BOOTSTRAP_SERVERS = "localhost:9092"

    POLL_TIMEOUT = 60_000


    def process_batch(msgs):
        # Group by key do simple count sampling by a minute window
        buckets_by_key = defaultdict(Counter)
        for msg in msgs:
            timestamp = (msg.timestamp // 60_000) * 60
            buckets_by_key[msg.key][timestamp] += 1

        res = []
        for key, counts in buckets_by_key.items():
            for timestamp, count in counts.items():
                value = str(count).encode()
                res.append((key, value, timestamp))

        return res


    async def transactional_process(loop):
        consumer = AIOKafkaConsumer(
            IN_TOPIC, loop=loop,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            enable_auto_commit=False,
            group_id=GROUP_ID,
            isolation_level="read_committed"  # <-- This will filter aborted txn's
        )
        await consumer.start()

        producer = AIOKafkaProducer(
            loop=loop, bootstrap_servers=BOOTSTRAP_SERVERS,
            transactional_id=TRANSACTIONAL_ID
        )
        await producer.start()

        try:
            while True:
                msg_batch = await consumer.getmany(timeout_ms=POLL_TIMEOUT)

                async with producer.transaction():
                    commit_offsets = {}
                    in_msgs = []
                    for tp, msgs in msg_batch.items():
                        in_msgs.extend(msgs)
                        commit_offsets[tp] = msgs[-1].offset + 1

                    out_msgs = process_batch(in_msgs)
                    for key, value, timestamp in out_msgs:
                        await producer.send(
                            OUT_TOPIC, value=value, key=key,
                            timestamp_ms=int(timestamp * 1000)
                        )
                    # We commit through the producer because we want the commit
                    # to only succeed if the whole transaction is done
                    # successfully.
                    await producer.send_offsets_to_transaction(
                        commit_offsets, GROUP_ID)
        finally:
            await consumer.stop()
            await producer.stop()

    def run_async(async_main):
        # Setup to properly handle KeyboardInterrupt exception
        loop = asyncio.get_event_loop()
        m_task = loop.create_task(async_main(loop))
        m_task.add_done_callback(lambda task, loop=loop: loop.stop())

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            m_task.cancel()
            loop.run_forever()
        finally:
            if not m_task.cancelled():
                m_task.result()

    if __name__ == "__main__":
        run_async(transactional_process)

