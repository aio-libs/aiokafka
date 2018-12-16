.. _producer-usage:

Producer client
===============

.. _delivery semantics: https://kafka.apache.org/documentation/#semantics

:ref:`AIOKafkaProducer <aiokafka-producer>` is a client that publishes records
to the Kafka cluster. Most simple usage would be::

    producer = aiokafka.AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092')
    await producer.start()
    try:
        await producer.send_and_wait("my_topic", b"Super message")
    finally:
        await producer.stop()

Under the hood, **Producer** does quite some work on message delivery including
batching, retries, etc. All of it can be configured, so let's go through some
components for a better understanding of the configuration options.


Message buffering
-----------------

While the user would expect the example above to send ``"Super message"``
directly to the broker, it's actually not sent right away, but rather added to
a **buffer space**. A background task will then get batches of messages and
send them to appropriate nodes in the cluster. This batching scheme allows
*more throughput and more efficient compression*. To see it more clearly lets
avoid ``send_and_wait`` shortcut::

    # Will add the message to 1st partition's batch. If this method times out,
    # we can say for sure that message will never be sent.

    fut = await producer.send("my_topic", b"Super message", partition=1)

    # Message will either be delivered or an unrecoverable error will occur.
    # Cancelling this future will not cancel the send.
    msg = await fut


Batches themselves are created **per partition** with a maximum size of
``max_batch_size``. Messages in a batch are strictly in append order and only
1 batch per partition is sent at a time (*aiokafka* does not support
``max.inflight.requests.per.connection`` option present in Java client). This
makes a strict guarantee on message order in a partition.

By default, a new batch is sent immediately after the previous one (even if
it's not full). If you want to reduce the number of requests you can set
``linger_ms`` to something other than 0. This will add an additional delay
before sending next batch if it's not yet full.

``aiokafka`` does not (yet!) support some options, supported by Java's client:

    * ``buffer.memory`` to limit how much buffer space is used by Producer to
      schedule requests in *all partitions*.
    * ``max.block.ms`` to limit the amount of time ``send()`` coroutine will
      wait for buffer append when the memory limit is reached. For now use::

        await asyncio.wait_for(producer.send(...), timeout=timeout)

If your use case requires direct batching control, see `Direct batch control`_.


Retries and Message acknowledgement
-----------------------------------

*aiokafka* will retry most errors automatically, but only until
``request_timeout_ms``. If a request is expired, the last error will be raised
to the application. Retrying messages on application level after an error
will potentially lead to duplicates, so it's up to the user to decide.

For example, if ``RequestTimedOutError`` is raised, Producer can't be sure if
the Broker wrote the request or not.

The ``acks`` option controls when the produce request is considered
acknowledged.

The most durable setting is ``acks="all"``. Broker will wait for all
available replicas to write the request before replying to Producer. Broker
will consult it's ``min.insync.replicas`` setting to know the minimal amount of
replicas to write. If there's not enough in sync replicas either
``NotEnoughReplicasError`` or ``NotEnoughReplicasAfterAppendError`` will be
raised. It's up to the user what to do in those cases, as the errors are not
retriable.

The default is ``ack=1`` setting. It will not wait for replica writes, only for
Leader to write the request.

The least safe is ``ack=0`` when there will be no acknowledgement from Broker,
meaning client will never retry, as it will never see any errors.


Indempotent produce
-------------------

As of Kafka 0.11 the Brokers support indempotent producing, that will prevent
the Producer from creating duplicates on retries. *aiokafka* supports this mode
by passing the parameter ``enable_idempotence=True`` to ``AIOKafkaProducer``::

    producer = aiokafka.AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092',
        enable_idempotence=True)
    await producer.start()
    try:
        await producer.send_and_wait("my_topic", b"Super message")
    finally:
        await producer.stop()

This option will change a bit the logic on message delivery:

    * The above mentioned ``ack="all"`` will be forced. If any other value is
      explicitly passed with ``enable_idempotence=True`` a ``ValueError`` will
      be raised.
    * I contrast to general mode, will not raise ``RequestTimedOutError``
      errors and will not expire batch delivery after ``request_timeout_ms``
      passed.

.. versionadded:: 0.5.0


.. _transactional-producer:

Transactional producer
----------------------

As of Kafka 0.11 the Brokers support transactional message producer, meaning
that messages sent to one or more topics will only be visible on consumers
after the transaction is committed. To use the transactional producer and the
attendant APIs, you must set the ``transactional_id`` configuration property::

    producer = aiokafka.AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092',
        transactional_id="transactional_test")
    await producer.start()
    try:
        async with producer.transaction():
            res = await producer.send_and_wait(
                "test-topic", b"Super transactional message")
    finally:
        await producer.stop()

If the ``transactional_id`` is set, idempotence is automatically enabled along
with the producer configs which idempotence depends on. Further, topics which
are included in transactions should be configured for durability. In
particular, the ``replication.factor`` should be at least ``3``, and the
``min.insync.replicas`` for these topics should be set to ``2``. Finally, in
order for transactional guarantees to be realized from end-to-end, the
consumers must be configured to read only committed messages as well. See 
:ref:`Reading Transactional Messages <transactional-consume>`.

The purpose of the ``transactional_id`` is to enable transaction recovery
across  multiple sessions of a single producer instance. It would typically be
derived from the shard identifier in a partitioned, stateful, application. As
such, it should be unique to each producer instance running within a
partitioned application. Using the same ``transactional_id`` will cause the
previous instance to raise an exception ``ProducerFenced`` that is not
retriable and will force it to exit.

Besides the ``transaction()`` shortcut producer also supports a set of API's
similar to ones in Java Client. See :ref:`AIOKafkaProducer <aiokafka-producer>`
API docs.

Besides being able to commit several topics atomically, as offsets are also
stored in a separate system topic it's possible to commit a consumer offset as
part of the same transaction::

    async with producer.transaction():
        commit_offsets = {
            TopicPartition("some-topic", 0): 100
        }
        await producer.send_offsets_to_transaction(
            commit_offsets, "some-consumer-group")

See a more full example in
:ref:`Transactional Consume-Process-Produce <transaction-example>`.

.. versionadded:: 0.5.0


Returned RecordMetadata object
------------------------------

After a message is sent the user receives a ``RecordMetadata`` object
containing fields:

    * ``offset`` - unique offset of the message in this partition. See 
      :ref:`Offsets and Consumer Position <offset_and_position>` for
      more details on offsets.
    * ``topic`` - *string* topic name
    * ``partition`` - *int* partition number
    * ``timestamp`` - *int* timestamp in epoch milliseconds (from Jan 1 1970
      UTC)
    * ``timestamp_type`` - *int* if broker respected the timestamp passed to
      ``send()`` 0 will be returned (CreateTime). If Broker set it's own
      timestamp 1 will be returned (LogAppendTime).

.. note:: In a very rare case, when Indempotent or Transactional producer is
    used and there was a long wait between batch initial send and a retry,
    producer may return ``offset == -1`` and ``timestamp == -1`` as Broker
    already expired the metadata for this produce sequence and only knows that
    it's a duplicate due to a larger sequence present

Direct batch control
--------------------

Users who need precise control over batch flow may use the lower-level
``create_batch()`` and ``send_batch()`` interfaces::

    # Create the batch without queueing for delivery.
    batch = producer.create_batch()

    # Populate the batch. The append() method will return metadata for the
    # added message or None if batch is full.
    for i in range(2):
        metadata = batch.append(value=b"msg %d" % i, key=None, timestamp=None)
        assert metadata is not None

    # Optionaly close the batch to further submission. If left open, the batch
    # may be appended to by producer.send().
    batch.close()

    # Add the batch to the first partition's submission queue. If this method
    # times out, we can say for sure that batch will never be sent.
    fut = await producer.send_batch(batch, "my_topic", partition=1)

    # Batch will either be delivered or an unrecoverable error will occur.
    # Cancelling this future will not cancel the send.
    record = await fut

While any number of batches may be created, only a single batch per partition
is sent at a time. Additional calls to ``send_batch()`` against the same
partition will wait for the inflight batch to be delivered before sending.

Upon delivery, ``record.offset`` will match the batch's first message.
