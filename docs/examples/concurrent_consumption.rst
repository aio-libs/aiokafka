Concurrent consumption
======================

In Kafka, partitions are the unit of parallelism: consumption is supposed to be 
sequential within a partition to maintain partial ordering and can be concurrent between 
partitions for increased throughput.

Partial ordering refers to a situation when messages sharing the same paritioning 
key maintain their relative order. For example, if user ID is used as a partitioning 
key, messages relating to the same user can be routed to the same partition, thus 
ensuring that they can be processed in the same order in which they are sent.

Concurrent consumption from a single partition would be tricky because of lack of 
out-of-order acknowledgements: when two messages arrive, one after the other, if the second 
message is for some reason processed before the first one, its offset can not yet 
be committed as that would effectively also commit the first message.

The most straightforward way to achieve consumption that is sequential within a partition 
and concurrent between partitions when there are N partitions is to spawn N consumers, and 
for each to run a loop that sequentially processes messages from a single partition, and 
to run those loops concurrently.

.. code:: python

        import asyncio
        import json
        import logging
        import signal

        from aiokafka import AIOKafkaConsumer, ConsumerRecord

        from utils import MultiLock

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        logging.getLogger("aiokafka").setLevel(logging.WARNING)
        logger = logging.getLogger(__name__)


        async def process_message(message: ConsumerRecord) -> None:
            try:
                body = json.loads(message.value.decode())
                await asyncio.sleep(3)
                logger.info({"id": body["id"]})
            except Exception as exc:
                logger.error("Message %s processing failed", message, exc_info=exc)


        async def run_consumer(
            consumer: AIOKafkaConsumer,
            is_stopping_event: asyncio.Event,
            not_safe_to_stop_lock: MultiLock,
        ) -> None:
            consumer.subscribe(["topic1"])
            await consumer.start()
            logger.info(
                f"Consumer {consumer._coordinator.member_id} assigned to partitions: "
                f"{consumer.assignment()}"
            )

            while not is_stopping_event.is_set():
                message = await consumer.getone()
                with not_safe_to_stop_lock:
                    await process_message(message)
                    await consumer.commit()


        async def main(num_workers: int = 4) -> None:
            consumers = [
                AIOKafkaConsumer(enable_auto_commit=False, group_id="group1")
                for _ in range(num_workers)
            ]

            is_stopping_event = asyncio.Event()
            not_safe_to_stop_lock = MultiLock()

            consuming_tasks = [
                asyncio.create_task(
                    run_consumer(consumer, is_stopping_event, not_safe_to_stop_lock)
                )
                for consumer in consumers
            ]

            loop = asyncio.get_event_loop()
            for signal_ in [signal.SIGINT, signal.SIGTERM]:
                loop.add_signal_handler(signal_, lambda: is_stopping_event.set())

            await is_stopping_event.wait()
            await not_safe_to_stop_lock.released()
            [task.cancel() for task in consuming_tasks]
            await asyncio.gather(*[consumer.stop() for consumer in consumers])


        asyncio.run(main())

This spawns 4 concurrent consumers. If the topic has 4 partitions, each consumer gets assigned
a partition::

    2025-02-15 19:28:28,679 - INFO - Consumer aiokafka-0.10.0-75052b60-68cc-4c2d-9e8c-b60a20b71107 assigned to partitions: frozenset({TopicPartition(topic='topic1', partition=2)})
    2025-02-15 19:28:28,679 - INFO - Consumer aiokafka-0.10.0-3ac2c7d0-084d-455e-bfa3-3470c08b0261 assigned to partitions: frozenset({TopicPartition(topic='topic1', partition=1)})
    2025-02-15 19:28:28,680 - INFO - Consumer aiokafka-0.10.0-db5f0d2c-e323-4acf-bccd-6a0930a371ee assigned to partitions: frozenset({TopicPartition(topic='topic1', partition=3)})
    2025-02-15 19:28:28,680 - INFO - Consumer aiokafka-0.10.0-13e9f6b4-99f8-4e81-8178-8a9e788856cd assigned to partitions: frozenset({TopicPartition(topic='topic1', partition=0)})

If we produce 8 messages into the topic using round-robin partitioning, we will see that first the
first 4 messages are processed concurrently and then the next 4 messages are processed concurrently::

    2025-02-15 19:28:36,426 - INFO - {'id': 0}
    2025-02-15 19:28:36,528 - INFO - {'id': 1}
    2025-02-15 19:28:36,633 - INFO - {'id': 2}
    2025-02-15 19:28:36,737 - INFO - {'id': 3}
    2025-02-15 19:28:39,436 - INFO - {'id': 4}
    2025-02-15 19:28:39,536 - INFO - {'id': 5}
    2025-02-15 19:28:39,641 - INFO - {'id': 6}
    2025-02-15 19:28:39,744 - INFO - {'id': 7}

``MultiLock`` is a helper to ensure consumers are not stopped while a message is mid-processing:

.. code:: python
    
        import asyncio
        from contextlib import suppress
        from types import TracebackType
        from typing import Type


        class MultiLock:
            def __init__(self) -> None:
                self.queue: asyncio.Queue[None] = asyncio.Queue()

            def __enter__(self) -> None:
                self.queue.put_nowait(None)

            def __exit__(
                self,
                exc_type: Type[BaseException] | None,
                exc_value: BaseException | None,
                exc_traceback: TracebackType | None,
            ) -> None:
                with suppress(asyncio.QueueEmpty, ValueError):
                    self.queue.get_nowait()
                    self.queue.task_done()

            async def released(self) -> None:
                await self.queue.join()
