import asyncio
import json
import logging
import signal
from contextlib import suppress
from types import TracebackType
from typing import Type

from aiokafka import AIOKafkaConsumer, ConsumerRecord

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


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
