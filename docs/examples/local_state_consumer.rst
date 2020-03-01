.. _local_state_consumer_example:

Local state and storing offsets outside of Kafka
================================================

While the default for Kafka applications is storing commit points in Kafka's
internal storage, you can disable that and use `seek()` to move to stored
points. This makes sense if you want to store offsets in the same system as
results of computations (filesystem in example below). But that said, you will
still probably want to use the coordinated consumer groups feature.

This example shows extensive usage of ``ConsumerRebalanceListener`` to control
what's done before and after rebalance's.

Local State consumer:

.. code:: python

    import asyncio
    from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
    from aiokafka.errors import OffsetOutOfRangeError


    import json
    import pathlib
    from collections import Counter

    FILE_NAME_TMPL = "/tmp/my-partition-state-{tp.topic}-{tp.partition}.json"


    class RebalanceListener(ConsumerRebalanceListener):

        def __init__(self, consumer, local_state):
            self.consumer = consumer
            self.local_state = local_state

        async def on_partitions_revoked(self, revoked):
            print("Revoked", revoked)
            self.local_state.dump_local_state()

        async def on_partitions_assigned(self, assigned):
            print("Assigned", assigned)
            self.local_state.load_local_state(assigned)
            for tp in assigned:
                last_offset = self.local_state.get_last_offset(tp)
                if last_offset < 0:
                    await self.consumer.seek_to_beginning(tp)
                else:
                    self.consumer.seek(tp, last_offset + 1)


    class LocalState:

        def __init__(self):
            self._counts = {}
            self._offsets = {}

        def dump_local_state(self):
            for tp in self._counts:
                fpath = pathlib.Path(FILE_NAME_TMPL.format(tp=tp))
                with fpath.open("w+") as f:
                    json.dump({
                        "last_offset": self._offsets[tp],
                        "counts": dict(self._counts[tp])
                    }, f)

        def load_local_state(self, partitions):
            self._counts.clear()
            self._offsets.clear()
            for tp in partitions:
                fpath = pathlib.Path(FILE_NAME_TMPL.format(tp=tp))
                state = {
                    "last_offset": -1,  # Non existing, will reset
                    "counts": {}
                }
                if fpath.exists():
                    with fpath.open("r+") as f:
                        try:
                            state = json.load(f)
                        except json.JSONDecodeError:
                            pass
                self._counts[tp] = Counter(state['counts'])
                self._offsets[tp] = state['last_offset']

        def add_counts(self, tp, counts, last_offset):
            self._counts[tp] += counts
            self._offsets[tp] = last_offset

        def get_last_offset(self, tp):
            return self._offsets[tp]

        def discard_state(self, tps):
            for tp in tps:
                self._offsets[tp] = -1
                self._counts[tp] = Counter()


    async def save_state_every_second(local_state):
        while True:
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                break
            local_state.dump_local_state()


    async def consume(loop):
        consumer = AIOKafkaConsumer(
            loop=loop, bootstrap_servers='localhost:9092',
            group_id="my_group",           # Consumer must be in a group to commit
            enable_auto_commit=False,      # Will disable autocommit
            auto_offset_reset="none",
            key_deserializer=lambda key: key.decode("utf-8") if key else "",
        )
        await consumer.start()

        local_state = LocalState()
        listener = RebalanceListener(consumer, local_state)
        consumer.subscribe(topics=["test"], listener=listener)

        save_task = loop.create_task(save_state_every_second(local_state))

        try:

            while True:
                try:
                    msg_set = await consumer.getmany(timeout_ms=1000)
                except OffsetOutOfRangeError as err:
                    # This means that saved file is outdated and should be
                    # discarded
                    tps = err.args[0].keys()
                    local_state.discard_state(tps)
                    await consumer.seek_to_beginning(*tps)
                    continue

                for tp, msgs in msg_set.items():
                    counts = Counter()
                    for msg in msgs:
                        print("Process", tp, msg.key)
                        counts[msg.key] += 1
                    local_state.add_counts(tp, counts, msg.offset)

        finally:
            await consumer.stop()
            save_task.cancel()
            await save_task


    def main(async_main):
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
        main(consume)

There are several points of interest in this example:

  * We implement ``RebalanceListener`` to dump all counts and offsets before
    rebalances. After rebalances we load them from the same files. It's a kind
    of cache to avoid re-reading all messages.
  * We control offset reset policy manually by setting
    ``auto_offset_reset="none"``. We need it to catch OffsetOutOfRangeError
    so we can clear cache if files were old and such offsets don't exist
    anymore in Kafka.
  * As we count ``keys`` here, those will always be partitioned to the same
    partition on produce. We will not have duplicate counts in different files.


Output for 1st consumer:

>>> python examples/local_state_consumer.py
Revoked set()
Assigned {TopicPartition(topic='test', partition=0), TopicPartition(topic='test', partition=1), TopicPartition(topic='test', partition=2)}
Heartbeat failed for group my_group because it is rebalancing
Revoked {TopicPartition(topic='test', partition=0), TopicPartition(topic='test', partition=1), TopicPartition(topic='test', partition=2)}
Assigned {TopicPartition(topic='test', partition=0), TopicPartition(topic='test', partition=2)}
Process TopicPartition(topic='test', partition=2) 123
Process TopicPartition(topic='test', partition=2) 9999
Process TopicPartition(topic='test', partition=2) 1111
Process TopicPartition(topic='test', partition=0) 4444
Process TopicPartition(topic='test', partition=0) 123123
Process TopicPartition(topic='test', partition=0) 5555
Process TopicPartition(topic='test', partition=2) 88891823
Process TopicPartition(topic='test', partition=2) 2

Output for 2nd consumer:

>>> python examples/local_state_consumer.py 
Revoked set()
Assigned {TopicPartition(topic='test', partition=1)}
Process TopicPartition(topic='test', partition=1) 321
Process TopicPartition(topic='test', partition=1) 777


Those create such files as a result:

>>> cat /tmp/my-partition-state-test-0.json && echo
{"last_offset": 4, "counts": {"123123": 1, "4444": 1, "321": 2, "5555": 1}}

