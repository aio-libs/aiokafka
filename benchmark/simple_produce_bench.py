import argparse
import asyncio
import signal
from aiokafka import AIOKafkaProducer
from collections import Counter
import random


class Benchmark:

    def __init__(self, args):
        self._num = args.num
        self._size = args.size
        self._topic = args.topic
        self._producer_kwargs = dict(
            linger_ms=args.linger_ms,
            max_batch_size=args.batch_size,
            bootstrap_servers=args.broker_list,
        )
        if args.enable_idempotence:
            self._producer_kwargs['enable_idempotence'] = True

        if args.transactional_id:
            self._producer_kwargs['transactional_id'] = args.transactional_id
            self._is_transactional = True
        else:
            self._is_transactional = False

        self.transaction_size = args.transaction_size

        self._partition = args.partition
        self._stats_interval = 1
        self._stats = [Counter()]

    async def _stats_report(self, start):
        loop = asyncio.get_event_loop()
        interval = self._stats_interval
        i = 1
        try:
            while True:
                await asyncio.sleep(
                    (start + i * interval) - loop.time())
                stats = self._stats[-1]
                self._stats.append(Counter())
                i += 1
                print(
                    "Produced {stats[count]} messages in {interval} second(s)."
                    .format(stats=stats, interval=interval)
                )
        except asyncio.CancelledError:
            stats = sum(self._stats, Counter())
            total_time = loop.time() - start
            print(
                "Total produced {stats[count]} messages in "
                "{time:.2f} second(s). Avg {avg} m/s".format(
                    stats=stats,
                    time=total_time,
                    avg=stats['count'] // total_time
                )
            )

    async def bench_simple(self):
        payload = bytearray(b"m" * self._size)
        topic = self._topic
        partition = self._partition
        loop = asyncio.get_event_loop()

        producer = AIOKafkaProducer(loop=loop, **self._producer_kwargs)
        await producer.start()

        # We start from after producer connect
        reporter_task = loop.create_task(self._stats_report(loop.time()))
        transaction_size = self.transaction_size

        try:
            if not self._is_transactional:
                for i in range(self._num):
                    # payload[i % self._size] = random.randint(0, 255)
                    await producer.send(topic, payload, partition=partition)
                    self._stats[-1]['count'] += 1
            else:
                for i in range(self._num // transaction_size):
                    # payload[i % self._size] = random.randint(0, 255)
                    async with producer.transaction():
                        for _ in range(transaction_size):
                            await producer.send(
                                topic, payload, partition=partition)
                            self._stats[-1]['count'] += 1
        except asyncio.CancelledError:
            pass
        finally:
            await producer.stop()
            reporter_task.cancel()
            await reporter_task


def parse_args():
    parser = argparse.ArgumentParser(
        description='Benchmark for maximum throughput to broker on produce')
    parser.add_argument(
        '-b', '--broker-list', default="localhost:9092",
        help='List of bootstrap servers. Default {default}.')
    parser.add_argument(
        '-n', '--num', type=int, default=100000,
        help='Number of messagess to send. Default {default}.')
    parser.add_argument(
        '-s', '--size', type=int, default=100,
        help='Size of message payload in bytes. Default {default}.')
    parser.add_argument(
        '--batch-size', type=int, default=16384,
        help='`max_batch_size` attr of Producer. Default {default}.')
    parser.add_argument(
        '--linger-ms', type=int, default=0,
        help='`linger_ms` attr of Producer. Default {default}.')
    parser.add_argument(
        '--topic', default="test",
        help='Topic to produce messages to. Default {default}.')
    parser.add_argument(
        '--partition', type=int, default=0,
        help='Partition to produce messages to. Default {default}.')
    parser.add_argument(
        '--uvloop', action='store_true',
        help='Use uvloop instead of asyncio default loop.')
    parser.add_argument(
        '--enable-idempotence', action='store_true',
        help='If producer should be set up with `enable_idempotence`')
    parser.add_argument(
        '--transactional-id',
        help='To enable transactional producer')
    parser.add_argument(
        '--transaction-size', type=int, default=100,
        help='Number of messages in transaction')
    return parser.parse_args()


def main():
    args = parse_args()
    if args.uvloop:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    loop = asyncio.get_event_loop()
    task = loop.create_task(Benchmark(args).bench_simple())
    task.add_done_callback(lambda _, loop=loop: loop.stop())

    def signal_hndl(_task=task):
        _task.cancel()
    loop.add_signal_handler(signal.SIGTERM, signal_hndl)
    loop.add_signal_handler(signal.SIGINT, signal_hndl)

    try:
        loop.run_forever()
    finally:
        loop.close()
        if not task.cancelled():
            task.result()


if __name__ == "__main__":
    main()
