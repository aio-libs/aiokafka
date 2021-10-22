#!/usr/bin/env python3
import pyperf

from kafka.partitioner.hashed import murmur2 as murmur2_kafka
from aiokafka.partitioner import murmur2


def run_murmur2(loops: int):
    data = bytes(range(10))
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        murmur2(data)
    res = pyperf.perf_counter() - t0

    return res

def run_murmur2_kafka(loops: int):
    data = bytes(range(10))
    t0 = pyperf.perf_counter()
    for _ in range(loops):
        murmur2_kafka(data)
    res = pyperf.perf_counter() - t0

    return res


runner = pyperf.Runner()
runner.bench_time_func('murmur2 cython realization', run_murmur2)
runner.bench_time_func('murmur2 python realization', run_murmur2_kafka)
