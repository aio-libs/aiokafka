#!/usr/bin/env python3
import perf
from aiokafka.producer.message_accumulator import BatchBuilder
import itertools
import random


DEFAULT_BATCH_SIZE = 1600 * 1024
KEY_SIZE = 6
VALUE_SIZE = 60
TIMESTAMP_RANGE = [1505824130000, 1505824140000]

# With values above v1 record is 100 bytes, so 10_000 bytes for 100 messages
MESSAGES_PER_BATCH = 100


def random_bytes(length):
    buffer = bytearray(length)
    for i in range(length):
        buffer[i] = random.randint(0, 255)
    return bytes(buffer)


def func(loops: int, magic: int):
    # Jit can optimize out the whole function if the result is the same each
    # time
    precomputed_samples = iter(itertools.cycle([
        (random_bytes(KEY_SIZE),
         random_bytes(VALUE_SIZE),
         random.randint(*TIMESTAMP_RANGE)
         )
        for _ in range(int(MESSAGES_PER_BATCH * 1.94))
    ]))
    precomputed_sample = next(precomputed_samples)
    results = []

    t0 = perf.perf_counter()
    for _ in range(loops):
        batch = BatchBuilder(magic, batch_size=DEFAULT_BATCH_SIZE,
                             compression_type=None)
        for _ in range(MESSAGES_PER_BATCH):
            key, value, timestamp = precomputed_sample
            size = batch.append(timestamp=timestamp, key=key, value=value)
            assert size
        results.append(batch._build())

    res = perf.perf_counter() - t0
    return res


runner = perf.Runner()
runner.bench_time_func('batch_append_v0', func, 0)
runner.bench_time_func('batch_append_v1', func, 1)
