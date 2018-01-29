Benchmarks measured using `benchmark/simple_consume_bench.py` and
`simple_produce_bench.py` scripts.

Kafka environment:

* Broker 0.10.2.1
* Scala 2.11

Machine:

* OS Mac OSX 10.13 (17A405)
* 2.7 GHz Intel Core i7
* 8 GB 1600 MHz DDR3


0.3.1 (vanila asyncio):
Total consumed 1001600 messages in 24.43 second(s). Avg 40998.0 m/s.
Total produced 500000 messages in 24.38 second(s). Avg 20505.0 m/s

0.3.1 (with uvloop):
Total consumed 1001600 messages in 24.46 second(s). Avg 40953.0 m/s.
Total produced 500000 messages in 23.36 second(s). Avg 21400.0 m/s


master Pure Python (vanila asyncio):
Total consumed 1001600 messages in 18.93 second(s). Avg 52918.0 m/s.
Total produced 500000 messages in 19.75 second(s). Avg 25311.0 m/s

master Pure Python (with uvloop):
Total consumed 1001600 messages in 18.19 second(s). Avg 55057.0 m/s.
Total produced 500000 messages in 15.79 second(s). Avg 31675.0 m/s


master C ext (vanila asyncio):
Total consumed 1001600 messages in 4.99 second(s). Avg 200829.0 m/s.
Total produced 500000 messages in 13.48 second(s). Avg 37103.0 m/s

master C ext (with uvloop):
Total consumed 1001600 messages in 4.72 second(s). Avg 212248.0 m/s.
Total produced 500000 messages in 10.24 second(s). Avg 48828.0 m/s


We see an overal boost in speed. With C extension it's ~4.9X speedup on read
and ~1.8X on write. Without C extension we still have a good 29% boost on read
and 23% on write.
