#!/usr/bin/env python3
import perf
from aiokafka.producer.message_accumulator import BatchBuilder
from aiokafka.record.memory_records import MemoryRecords
import itertools
import random
import hashlib
import os


DEFAULT_BATCH_SIZE = 1600 * 1024
KEY_SIZE = 6
VALUE_SIZE = 60
TIMESTAMP_RANGE = [1505824130000, 1505824140000]

# With values above v1 record is 100 bytes, so 10_000 bytes for 100 messages
BATCH_SAMPLES = 5
MESSAGES_PER_BATCH = 100


def random_bytes(length):
    buffer = bytearray(length)
    for i in range(length):
        buffer[i] = random.randint(0, 255)
    return bytes(buffer)


def prepare(magic: int):
    samples = []
    for _ in range(BATCH_SAMPLES):
        batch = BatchBuilder(
            magic, batch_size=DEFAULT_BATCH_SIZE, compression_type=0)
        for offset in range(MESSAGES_PER_BATCH):
            size = batch.append(
                timestamp=None,  # random.randint(*TIMESTAMP_RANGE)
                key=random_bytes(KEY_SIZE),
                value=random_bytes(VALUE_SIZE))
            assert size
        samples.append(bytes(batch._builder.build()))

    return iter(itertools.cycle(samples))


def finalize(results):
    # Just some strange code to make sure PyPy does execute the code above
    # properly
    hash_val = hashlib.md5()
    for buf in results:
        hash_val.update(buf)
    print(hash_val, file=open(os.devnull, "w"))


def func(loops: int, magic: int):
    # Jit can optimize out the whole function if the result is the same each
    # time, so we need some randomized input data )
    precomputed_samples = prepare(magic)
    results = []

    # Main benchmark code.
    batch_data = next(precomputed_samples)
    t0 = perf.perf_counter()
    for _ in range(loops):
        records = MemoryRecords(batch_data)
        while records.has_next():
            batch = records.next_batch()
            batch.validate_crc()
            for record in batch:
                results.append(record.value)

    res = perf.perf_counter() - t0
    finalize(results)

    return res


runner = perf.Runner()
runner.bench_time_func('batch_read_v0', func, 0)
runner.bench_time_func('batch_read_v1', func, 1)
runner.bench_time_func('batch_read_v2', func, 2)
