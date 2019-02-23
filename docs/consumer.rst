.. _consumer-usage:

Consumer client
===============

.. _delivery semantics: https://kafka.apache.org/documentation/#semantics

:ref:`AIOKafkaConsumer <aiokafka-consumer>` is a client that consumes records
from a Kafka cluster. Most simple usage would be::

    consumer = aiokafka.AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={} timestamp_ms={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value,
                    msg.timestamp)
            )
    finally:
        await consumer.stop()

.. note:: ``msg.value`` and ``msg.key`` are raw bytes, use **key_deserializer**
  and **value_deserializer** configuration if you need to decode them. 

.. note:: **Consumer** maintains TCP connections as well as a few background
  tasks to fetch data and coordinate assignments. Failure to call
  ``Consumer.stop()`` after consumer use `will leave background tasks running`.

**Consumer** transparently handles the failure of Kafka brokers and
transparently adapts as topic partitions it fetches migrate within the
cluster. It also interacts with the broker to allow groups of consumers to load
balance consumption using **Consumer Groups**.


.. _offset_and_position:

Offsets and Consumer Position
-----------------------------

Kafka maintains a numerical *offset* for each record in a partition. This 
*offset* acts as a `unique identifier` of a record within that partition and
also denotes the *position* of the consumer in the partition. For example::

    msg = await consumer.getone()
    print(msg.offset)  # Unique msg autoincrement ID in this topic-partition.

    tp = aiokafka.TopicPartition(msg.topic, msg.partition)

    position = await consumer.position(tp)
    # Position is the next fetched offset
    assert position == msg.offset + 1

    committed = await consumer.committed(tp)
    print(committed)

.. note::
    To use ``consumer.commit()`` and ``consumer.committed()`` API you need
    to set ``group_id`` to something other than ``None``. See
    `Consumer Groups and Topic Subscriptions`_ below.

Here if the consumer is at *position* **5** it has consumed records with 
*offsets* **0** through **4** and will next receive the record with 
*offset* **5**.

There are actually two *notions of position*:

 * The *position* gives the *offset* of the next record that should be given
   out. It will be `one larger` than the highest *offset* the consumer
   has seen in that partition. It automatically increases every time the
   consumer yields messages in either `getmany()` or `getone()` calls.
 * The *committed position* is the last *offset* that has been stored securely.
   Should the process restart, this is the offset that the consumer will start
   from. The consumer can either `automatically commit offsets periodically`,
   or it can choose to control this committed position `manually` by calling
   ``await consumer.commit()``.

This distinction gives the consumer control over when a record is considered
consumed. It is discussed in further detail below.


Manual vs automatic committing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most simple use cases auto committing is probably the best choice::

    consumer = AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092',
        group_id="my_group",           # Consumer must be in a group to commit
        enable_auto_commit=True,       # Is True by default anyway
        auto_commit_interval_ms=1000,  # Autocommit every second
        auto_offset_reset="earliest",  # If committed offset not found, start
                                       # from beginnig
    )
    await consumer.start()

    async for msg in consumer:  # Will periodically commit returned messages.
        # process message
        pass

This example can have `"At least once"` `delivery semantics`_, but only if we
process messages **one at a time**. If you want `"At least once"` semantics for
batch operations you should use *manual commit*::

    consumer = AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092',
        group_id="my_group",           # Consumer must be in a group to commit
        enable_auto_commit=False,      # Will disable autocommit
        auto_offset_reset="earliest",  # If committed offset not found, start
                                       # from beginnig
    )
    await consumer.start()

    batch = []
    async for msg in consumer:
        batch.append(msg)
        if len(batch) == 100:
            await process_msg_batch(batch)
            await consumer.commit()
            batch = []

.. warning:: When using **manual commit** it is recommended to provide a
  :ref:`ConsumerRebalanceListener <consumer-rebalance-listener>` which will
  process pending messages in the batch and commit before allowing rejoin.
  If your group will rebalance during processing commit will fail with
  ``CommitFailedError``, as partitions may have been processed by other
  consumer already.

This example will hold on to messages until we have enough to process in
bulk. The algorithm can be enhanced by taking advantage of:

  * ``await consumer.getmany()`` to avoid multiple calls to get a batch of 
    messages.
  * ``await consumer.highwater(partition)`` to understand if we have more
    unconsumed messages or this one is the last one in the partition.

If you want to have more control over which partition and message is
committed, you can specify offset manually::

    while True:
        result = await consumer.getmany(timeout_ms=10 * 1000)
        for tp, messages in result.items():
            if messages:
                await process_msg_batch(messages)
                # Commit progress only for this partition
                await consumer.commit({tp: messages[-1].offset + 1})

.. note:: The committed offset should always be the offset of the next message
  that your application will read. Thus, when calling ``commit(offsets)`` you
  should add one to the offset of the last message processed.

Here we process a batch of messages per partition and commit not all consumed
*offsets*, but only for the partition, we processed.


Controlling The Consumer's Position
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In most use cases the consumer will simply consume records from beginning to
end, periodically committing its position (either automatically or manually).
If you only want your consumer to process newest messages, you can ask it to
start from `latest` offset::

    consumer = AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092',
        auto_offset_reset="latest",
    )
    await consumer.start()

    async for msg in consumer:
        # process message
        pass

.. note:: If you have a valid **committed position** consumer will use that.
  ``auto_offset_reset`` will only be used when the position is invalid.

Kafka also allows the consumer to manually control its position, moving
forward or backwards in a partition at will using ``consumer.seek()``.
For example, you can re-consume records::

    msg = await consumer.getone()
    tp = TopicPartition(msg.topic, msg.partition)

    consumer.seek(tp, msg.offset)
    msg2 = await consumer.getone()

    assert msg2 == msg

Also you can combine it with `offset_for_times` API to query to specific
offsets based on timestamp.

There are several use cases where manually controlling the consumer's position
can be useful.

*One case* is for **time-sensitive record processing** it may make sense for a
consumer that falls far enough behind to not attempt to catch up processing all
records, but rather just skip to the most recent records. Or you can use
``offsets_for_times`` API to get the offsets after certain timestamp.

*Another use case* is for a **system that maintains local state**. In such a
system the consumer will want to initialize its position on startup to
whatever is contained in the local store. Likewise, if the local state is 
destroyed (say because the disk is lost) the state may be recreated on a new
machine by re-consuming all the data and recreating the state (assuming that 
Kafka is retaining sufficient history).

See also related configuration params and API docs:

    * `auto_offset_reset` config option to set behaviour in case the position
      is either undefined or incorrect.
    * :meth:`seek <aiokafka.AIOKafkaConsumer.seek>`,
      :meth:`seek_to_beginning <aiokafka.AIOKafkaConsumer.seek_to_beginning>`,
      :meth:`seek_to_end <aiokafka.AIOKafkaConsumer.seek_to_end>`
      API's to force position change on partition('s).
    * :meth:`offsets_for_times <aiokafka.AIOKafkaConsumer.offsets_for_times>`,
      :meth:`beginning_offsets <aiokafka.AIOKafkaConsumer.beginning_offsets>`,
      :meth:`end_offsets <aiokafka.AIOKafkaConsumer.end_offsets>`
      API's to query offsets for partitions even if they are not assigned to
      this consumer.


Storing Offsets Outside Kafka
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Storing *offsets* in Kafka is optional, you can store offsets in another place
and use ``consumer.seek()`` API to start from saved position. The primary use
case for this is allowing the application to store both the offset and the
results of the consumption in the same system in a way that both the results
and offsets are stored atomically. For example, if we save aggregated by `key`
counts in Redis::

    import json
    from collections import Counter

    redis = await aioredis.create_redis(("localhost", 6379))
    REDIS_HASH_KEY = "aggregated_count:my_topic:0"

    tp = TopicPartition("my_topic", 0)
    consumer = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092',
        enable_auto_commit=False,
    )
    await consumer.start()
    consumer.assign([tp])

    # Load initial state of aggregation and last processed offset
    offset = -1
    counts = Counter()
    initial_counts = await redis.hgetall(REDIS_HASH_KEY, encoding="utf-8")
    for key, state in initial_counts.items():
        state = json.loads(state)
        offset = max([offset, state['offset']])
        counts[key] = state['count']

    # Same as with manual commit, you need to fetch next message, so +1
    consumer.seek(tp, offset + 1)

    async for msg in consumer:
        key = msg.key.decode("utf-8")
        counts[key] += 1
        value = json.dumps({
            "count": counts[key],
            "offset": msg.offset
        })
        await redis.hset(REDIS_HASH_KEY, key, value)

So to save results outside of Kafka you need to:

* Configure enable.auto.commit=false
* Use the offset provided with each ConsumerRecord to save your position
* On restart or rebalance restore the position of the consumer using
  ``consumer.seek()``

This is not always possible, but when it is it will make the consumption fully
atomic and give "exactly once" semantics that are stronger than the default
"at-least once" semantics you get with Kafka's offset commit functionality.

This type of usage is simplest when the partition assignment is also done
manually (like we did above). If the partition assignment is done automatically
special care is needed to handle the case where partition assignments change.
See :ref:`Local state and storing offsets outside of Kafka <local_state_consumer_example>`
example for more details.

Consumer Groups and Topic Subscriptions
---------------------------------------

Kafka uses the concept of **Consumer Groups** to allow a pool of processes to
divide the work of consuming and processing records. These processes can either
be running on the same machine or they can be distributed over many machines to
provide scalability and fault tolerance for processing. 

All **Consumer** instances sharing the same ``group_id`` will be part of the
same **Consumer Group**::

    # Process 1
    consumer = AIOKafkaConsumer(
        "my_topic", loop=loop, bootstrap_servers='localhost:9092',
        group_id="MyGreatConsumerGroup"  # This will enable Consumer Groups
    )
    await consumer.start()
    async for msg in consumer:
        print("Process %s consumed msg from partition %s" % (
              os.getpid(), msg.partition))

    # Process 2
    consumer2 = AIOKafkaConsumer(
        "my_topic", loop=loop, bootstrap_servers='localhost:9092',
        group_id="MyGreatConsumerGroup"  # This will enable Consumer Groups
    )
    await consumer2.start()
    async for msg in consumer2:
        print("Process %s consumed msg from partition %s" % (
              os.getpid(), msg.partition))


Each consumer in a group can dynamically set the list of topics it wants to
subscribe to through ``consumer.subscribe(...)`` call. Kafka will deliver each
message in the subscribed topics to only one of the processes in each consumer
group. This is achieved by balancing the *partitions* between all members in
the consumer group so that **each partition is assigned to exactly one
consumer** in the group. So if there is a topic with *four* partitions and a
consumer group with *two* processes, each process would consume from *two*
partitions.

Membership in a consumer group is maintained dynamically: if a process fails, 
the partitions assigned to it `will be reassigned to other consumers` in the 
same group. Similarly, if a new consumer joins the group, partitions will be 
`moved from existing consumers to the new one`. This is known as **rebalancing 
the group**.

.. note:: Conceptually you can think of a **Consumer Group** as being a `single 
   logical subscriber` that happens to be made up of multiple processes.

In addition, when group reassignment happens automatically, consumers can be
notified through a ``ConsumerRebalanceListener``, which allows them to finish
necessary application-level logic such as state cleanup, manual offset commits,
etc. See :meth:`aiokafka.AIOKafkaConsumer.subscribe` docs for more details.


.. warning:: Be careful with ``ConsumerRebalanceListener`` to avoid deadlocks.
    The Consumer will await the defined handlers and will block subsequent
    calls to `getmany()` and `getone()`. For example this code will deadlock::

        lock = asyncio.Lock()
        consumer = AIOKafkaConsumer(...)

        class MyRebalancer(aiokafka.ConsumerRebalanceListener):

            async def on_partitions_revoked(self, revoked):
                async with self.lock:
                    pass

            async def on_partitions_assigned(self, assigned):
                pass

        async def main():
            consumer.subscribe("topic", listener=MyRebalancer())
            while True:
                async with self.lock:
                    msgs = await consumer.getmany(timeout_ms=1000)
                    # process messages

    You need to put ``consumer.getmany(timeout_ms=1000)`` call outside of the
    lock.

For more information on how **Consumer Groups** are organized see 
`Official Kafka Docs <https://kafka.apache.org/documentation/#intro_consumers>`_.


Topic subscription by pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Consumer** performs periodic metadata refreshes in the background and will
notice when new partitions are added to one of the subscribed topics or when a
new topic matching a *subscribed regex* is created. For example::

    consumer = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092',
        metadata_max_age_ms=30000,  # This controlls the polling interval
    )
    await consumer.start()
    consumer.subscribe(pattern="^MyGreatTopic-.*$")

    async for msg in consumer:  # Will detect metadata changes
        print("Consumed msg %s %s %s" % (msg.topic, msg.partition, msg.value))

Here **Consumer** will automatically detect new topics like ``MyGreatTopic-1``
or ``MyGreatTopic-2`` and start consuming them.

If you use **Consumer Groups** the group's *Leader* will trigger a 
**group rebalance** when it notices metadata changes. It's because only the
*Leader* has full knowledge of which topics are assigned to the group.


Manual partition assignment
^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is also possible for the consumer to manually assign specific partitions 
using ``assign([tp1, tp2])``. In this case, dynamic partition assignment and
consumer group coordination will be disabled. For example::

    consumer = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092'
    )
    tp1 = TopicPartition("my_topic", 1)
    tp2 = TopicPartition("my_topic", 2)
    consumer.assign([tp1, tp2])

    async for msg in consumer:
        print("Consumed msg %s %s %s", msg.topic, msg.partition, msg.value)

``group_id`` can still be used for committing position, but be careful to 
avoid **collisions** with multiple instances sharing the same group.

It is not possible to mix manual partition assignment ``consumer.assign()`` 
and topic subscription ``consumer.subscribe()``. An attempt to do so will
result in an ``IllegalStateError``.


Consumption Flow Control
^^^^^^^^^^^^^^^^^^^^^^^^

By default Consumer will fetch from all partitions, effectively giving these
partitions the same priority. However in some cases, you would want for some
partitions to have higher priority (say they have more lag and you want to
catch up). For example::

    consumer = AIOKafkaConsumer("my_topic", ...)

    partitions = []  # Fetch all partitions on first request
    while True:
        msgs = await consumer.getmany(*partitions)
        # process messages
        await process_messages(msgs)

        # Prioritize partitions, that lag behind.
        partitions = []
        for partition in consumer.assignment():
            highwater = consumer.highwater(partition)
            position = await consumer.position(partition)
            lag = highwater - position
            if lag > LAG_THRESHOLD:
                partitions.append(partition)

.. note:: This interface differs from `pause()`/`resume()` interface of 
  `kafka-python` and Java clients.

Here we will consume all partitions if they do not lag behind, but if some
go above a certain *threshold*, we will consume them to catch up. This can
very well be used in a case where some consumer died and this consumer took
over its partitions, that are now lagging behind.

Some things to note about it:

* There may be a slight **pause in consumption** if you change the partitions
  you are fetching. This can happen when Consumer requests a fetch for
  partitions that have no data available. Consider setting a relatively low
  ``fetch_max_wait_ms`` to avoid this.
* The ``async for`` interface can not be used with explicit partition
  filtering, just use ``consumer.getone()`` instead.


.. _transactional-consume:

Reading Transactional Messages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Transactions were introduced in Kafka 0.11.0 wherein applications can write to
multiple topics and partitions atomically. In order for this to work, consumers
reading from these partitions should be configured to only read committed data.
This can be achieved by by setting the ``isolation_level=read_committed`` in
the consumer's configuration::

    consumer = aiokafka.AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092',
        isolation_level="read_committed"
    )
    await consumer.start()
    async for msg in consumer:  # Only read committed tranasctions
        pass

In `read_committed` mode, the consumer will read only those transactional
messages which have been successfully committed. It will continue to read
non-transactional messages as before. There is no client-side buffering in
`read_committed` mode. Instead, the end offset of a partition for a
`read_committed` consumer would be the offset of the first message in the
partition belonging to an open transaction. This offset is known as the 
**Last Stable Offset** (LSO).

A `read_committed` consumer will only read up to the LSO and filter out any
transactional messages which have been aborted. The LSO also affects the
behavior of ``seek_to_end(*partitions)`` and ``end_offsets(partitions)``
for ``read_committed`` consumers, details of which are in each method's
documentation. Finally, ``last_stable_offset()`` API was added similary to
``highwater()`` API to query the lSO on a currently assigned transaction::

    async for msg in consumer:  # Only read committed tranasctions
        tp = TopicPartition(msg.topic, msg.partition)
        lso = consumer.last_stable_offset(tp)
        lag = lso - msg.offset
        print(f"Consumer is behind by {lag} messages")

        end_offsets = await consumer.end_offsets([tp])
        assert end_offsets[tp] == lso

    await consumer.seek_to_end(tp)
    position = await consumer.position(tp)

Partitions with transactional messages will include commit or abort markers
which indicate the result of a transaction. There markers are not returned to
applications, yet have an offset in the log. As a result, applications reading
from topics with transactional messages will see gaps in the consumed offsets.
These missing messages would be the transaction markers, and they are filtered
out for consumers in both isolation levels. Additionally, applications using 
`read_committed` consumers may also see gaps due to aborted transactions, since
those messages would not be returned by the consumer and yet would have valid
offsets.


Detecting Consumer Failures
---------------------------

People who worked with ``kafka-python`` or Java Client probably know that
the ``poll()`` API is designed to ensure liveness of a **Consumer Group**. In
other words, Consumer will only be considered alive if it consumes messages.
It's not the same for ``aiokafka``, for more details read 
:ref:`Difference between aiokafka and kafka-python <kafka_python_difference>`.

``aiokafka`` will join the group on ``consumer.start()`` and will send
heartbeats in the background, keeping the group alive, same as Java Client.
But in the case of a rebalance it will also done in the background.

Offset commits in autocommit mode is done strictly by time in the background
(in Java client autocommit will not be done if you don't call ``poll()``
another time).
