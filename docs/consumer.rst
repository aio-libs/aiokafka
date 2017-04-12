.. _consumer-usage:

Consumer client
===============


:ref:`AIOKafkaConsumer <aiokafka-consumer>` is a client that consumes records
from a Kafka cluster. Most simple usage would be::

    consumer = AIOKafkaConsumer(
        "my_topic",
        loop=loop, bootstrap_servers='localhost:9092'
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(
                "{}:{:d}:{:d}: key={} value={}".format(
                    msg.topic, msg.partition, msg.offset, msg.key, msg.value)
            )
    finally:
        await consumer.stop()

.. note:: ``msg.value`` and ``msg.key`` are raw bytes, use **key_deserializer**
  and **value_deserializer** configuration if you need to decode them. 

.. note:: **Consumer** maintains TCP connections to the necessary brokers to
  fetch data. Failure to call ``Consumer.stop()`` after consumer use `will 
  leak these connections`.

**Consumer** transparently handles the failure of Kafka brokers, and
transparently adapts as topic partitions it fetches migrate within the
cluster. It also interacts with the broker to allow groups of consumers to load
balance consumption using **Consumer Groups**.


Offsets and Consumer Position
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kafka maintains a numerical *offset* for each record in a partition. This 
*offset* acts as a `unique identifier` of a record within that partition, and
also denotes the *position* of the consumer in the partition. For example::

    msg = await consumer.getone()
    msg.offset  # Unique msg autoincrement ID in this topic-partition.

    tp = TopicPartition(msg.topic, msg.partition)

    position = await consumer.position(tp)
    # Position is the next fetched offset
    assert position == msg.offset + 1

    committed = await consumet.committed(tp)

Here if the consumer is at *position* **5** it has consumed records with 
*offsets* **0** through **4** and will next receive the record with 
*offset* **5**.

There are actually two *notions of position*:

 * The *position* gives the *offset* of the next record that should be given
   out. It will be `one larger` than the highest *offset* the consumer
   has seen in that partition. It automatically advances every time the
   consumer yields messages in either `getmany()` or `getone()` calls.
 * The *committed position* is the last *offset* that has been stored securely.
   Should the process fail and restart, this is the offset that the consumer
   will recover to. The consumer can either `automatically commit offsets
   periodically`; or it can choose to control this committed position
   `manually` by calling ``await consumer.commit()``.

This distinction gives the consumer control over when a record is considered
consumed. It is discussed in further detail below.


Manual vs automatic committing
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For most simple use cases autocommiting is probably the best choice::

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

    async for msg in consumer:
        # process message
        pass

This example will have `"at least once"` delivery semantics, but only if we
process messages **one at a time**. If you want more controll over committing
you can use *manual commit*::

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

This examples will hold on to messages until we have enough to process in
bulk. The algorithm can be enhanced by taking advantage of:

  * ``await consumer.getmany()`` to avoid multiple calls for single message 
  * ``await consumer.highwater(partition)`` to understand if we have more
    unconsumed messages or this one is the last one in partition.

If you want to have more controll in which messages are to be committed, you
can specify offset manualy::

    while True:
        result = await consumer.getmany(timeout_ms=10 * 1000)
        for tp, messages in result.items():
            if messages:
                await process_msg_batch(messages)
                await consumer.commit({tp: messages[-1].offset + 1})

.. note:: The committed offset should always be the offset of the next message
  that your application will read. Thus, when calling ``commit(offsets)`` you 
  should add one to the offset of the last message processed.

Here we process a batch of messages per partition and commit not all consumed
*offsets*, but only for the partition we processed.


Controlling The Consumer's Position
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In most use cases the consumer will simply consume records from beginning to
end, periodically committing its position (either automatically or manually).
However Kafka allows the consumer to manually control its position, moving
forward or backwards in a partition at will. This means a consumer can 
re-consume older records, or skip to the most recent records without actually
consuming the intermediate records. For example you can reconsume record::

    msg = await consumer.getone()
    tp = TopicPartition(msg.topic, msg.partition)

    consumer.seek(tp, msg.offset)
    msg2 = await consumer.getone()

    assert msg2 == msg

There are several instances where manually controlling the consumer's position
can be useful.

*One case* is for time-sensitive record processing it may make sense for a
consumer that falls far enough behind to not attempt to catch up processing all
records, but rather just skip to the most recent records.

*Another use* case is for a system that maintains **local state**. In such a
system the consumer will want to initialize its position on start-up to
whatever is contained in the local store. Likewise if the local state is 
destroyed (say because the disk is lost) the state may be recreated on a new
machine by re-consuming all the data and recreating the state (assuming that 
Kafka is retaining sufficient history).


Consumer Groups and Topic Subscriptions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Kafka uses the concept of **Consumer Groups** to allow a pool of processes to
divide the work of consuming and processing records. These processes can either
be running on the same machine or they can be distributed over many machines to
provide scalability and fault tolerance for processing. 

All **Consumer** instances sharing the same ``group_id`` will be part of the
same **Consumer Group**::

    # Process 1
    consumer = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092',
        group_id="MyGreatConsumerGroup"  # This will enable Consumer Groups
    )
    await consumer.start()
    consumer.subscribe(["my_topic"])
    async for msg in consumer:
        print("Process %s consumed msg from partition %s" % (
              os.getpid(), msg.partition))

    # Process 2
    consumer2 = AIOKafkaConsumer(
        loop=loop, bootstrap_servers='localhost:9092',
        group_id="MyGreatConsumerGroup"  # This will enable Consumer Groups
    )
    await consumer2.start()
    consumer2.subscribe(["my_topic"])
    async for msg in consumer2:
        print("Process %s consumed msg from partition %s" % (
              os.getpid(), msg.partition))


Each consumer in a group can dynamically set the list of topics it wants to
subscribe to through ``consumer.subscribe(...)`` call. Kafka will deliver each
message in the subscribed topics to only one of the processes in each consumer
group. This is achieved by balancing the *partitions* between all members in
the consumer group so that **each partition is assigned to exactly one
consumer** in the group. So if there is a topic with *four* partitions, and a
consumer group with *two* processes, each process would consume from *two*
partitions.

Membership in a consumer group is maintained dynamically: if a process fails, 
the partitions assigned to it `will be reassigned to other consumers` in the 
same group. Similarly, if a new consumer joins the group, partitions will be 
`moved from existing consumers to the new one`. This is known as **rebalancing 
the group**.

.. note:: Conceptually you can think of a **Consumer Group** as being a `single 
   logical subscriber` that happens to be made up of multiple processes.

For more information on how **Consumer Groups** are organized see 
`Official Kafka Docs <https://kafka.apache.org/documentation/#intro_consumers>`_.


Topic subsciption by pattern
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

.. Consumption Flow Control

.. If a consumer is assigned multiple partitions to fetch data from, it will try to consume from all of them at the same time, effectively giving these partitions the same priority for consumption. However in some cases consumers may want to first focus on fetching from some subset of the assigned partitions at full speed, and only start fetching other partitions when these partitions have few or no data to consume.
.. One of such cases is stream processing, where processor fetches from two topics and performs the join on these two streams. When one of the topics is long lagging behind the other, the processor would like to pause fetching from the ahead topic in order to get the lagging stream to catch up. Another example is bootstraping upon consumer starting up where there are a lot of history data to catch up, the applications usually want to get the latest data on some of the topics before consider fetching other topics.

.. Kafka supports dynamic controlling of consumption flows by using pause(Collection) and resume(Collection) to pause the consumption on the specified assigned partitions and resume the consumption on the specified paused partitions respectively in the future poll(long) calls.


.. Detecting Consumer Failures

.. After subscribing to a set of topics, the consumer will automatically join the group when poll(long) is invoked. The poll API is designed to ensure consumer liveness. As long as you continue to call poll, the consumer will stay in the group and continue to receive messages from the partitions it was assigned. Underneath the covers, the consumer sends periodic heartbeats to the server. If the consumer crashes or is unable to send heartbeats for a duration of session.timeout.ms, then the consumer will be considered dead and its partitions will be reassigned.
.. It is also possible that the consumer could encounter a "livelock" situation where it is continuing to send heartbeats, but no progress is being made. To prevent the consumer from holding onto its partitions indefinitely in this case, we provide a liveness detection mechanism using the max.poll.interval.ms setting. Basically if you don't call poll at least as frequently as the configured max interval, then the client will proactively leave the group so that another consumer can take over its partitions. When this happens, you may see an offset commit failure (as indicated by a CommitFailedException thrown from a call to commitSync()). This is a safety mechanism which guarantees that only active members of the group are able to commit offsets. So to stay in the group, you must continue to call poll.

.. The consumer provides two configuration settings to control the behavior of the poll loop:

.. max.poll.interval.ms: By increasing the interval between expected polls, you can give the consumer more time to handle a batch of records returned from poll(long). The drawback is that increasing this value may delay a group rebalance since the consumer will only join the rebalance inside the call to poll. You can use this setting to bound the time to finish a rebalance, but you risk slower progress if the consumer cannot actually call poll often enough.
.. max.poll.records: Use this setting to limit the total records returned from a single call to poll. This can make it easier to predict the maximum that must be handled within each poll interval. By tuning this value, you may be able to reduce the poll interval, which will reduce the impact of group rebalancing.
.. For use cases where message processing time varies unpredictably, neither of these options may be sufficient. The recommended way to handle these cases is to move message processing to another thread, which allows the consumer to continue calling poll while the processor is still working. Some care must be taken to ensure that committed offsets do not get ahead of the actual position. Typically, you must disable automatic commits and manually commit processed offsets for records only after the thread has finished handling them (depending on the delivery semantics you need). Note also that you will need to pause the partition so that no new records are received from poll until after thread has finished handling those previously returned.


.. Multi-threaded Processing

.. The Kafka consumer is NOT thread-safe. All network I/O happens in the thread of the application making the call. It is the responsibility of the user to ensure that multi-threaded access is properly synchronized. Un-synchronized access will result in ConcurrentModificationException.
.. The only exception to this rule is wakeup(), which can safely be used from an external thread to interrupt an active operation. In this case, a WakeupException will be thrown from the thread blocking on the operation. This can be used to shutdown the consumer from another thread. The following snippet shows the typical pattern:

..  public class KafkaConsumerRunner implements Runnable {
..      private final AtomicBoolean closed = new AtomicBoolean(false);
..      private final KafkaConsumer consumer;

..      public void run() {
..          try {
..              consumer.subscribe(Arrays.asList("topic"));
..              while (!closed.get()) {
..                  ConsumerRecords records = consumer.poll(10000);
..                  // Handle new records
..              }
..          } catch (WakeupException e) {
..              // Ignore exception if closing
..              if (!closed.get()) throw e;
..          } finally {
..              consumer.close();
..          }
..      }

..      // Shutdown hook which can be called from a separate thread
..      public void shutdown() {
..          closed.set(true);
..          consumer.wakeup();
..      }
..  }
 
.. Then in a separate thread, the consumer can be shutdown by setting the closed flag and waking up the consumer.
..      closed.set(true);
..      consumer.wakeup();
 
.. We have intentionally avoided implementing a particular threading model for processing. This leaves several options for implementing multi-threaded processing of records.

