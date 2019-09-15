.. _kafka_python_difference:

Difference between aiokafka and kafka-python
--------------------------------------------

.. _kip-41:
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-41%3A+KafkaConsumer+Max+Records

.. _kip-62:
    https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread

.. _a lot of code:
  https://gist.github.com/tvoinarovskyi/05a5d083a0f96cae3e9b4c2af580be74

Why do we need another library?
===============================

``kafka-python`` is a great project, which tries to fully mimic the interface
of **Java Client API**. It is more *feature* oriented, rather than *speed*, but
still gives quite good throughput. It's actively developed and is fast to react
to changes in the Java client.

While ``kafka-python`` has a lot of great features it is made to be used in a
**Threaded** environment. Even more, it mimics Java's client, making it 
**Java's threaded** environment, which does not have that much of
`asynchronous` ways of doing things. It's not **bad** as Java's Threads are
very powerful with the ability to use multiple cores.

The API itself just can't be adopted to be used in an asynchronous way (even
though the library does asynchronous IO using `selectors`). It has too much
blocking behavior including `blocking` socket usage, threading synchronization,
etc. Examples would be:

  * `bootstrap`, which blocks in the constructor itself
  * blocking iterator for consumption
  * sending produce requests block if buffer is full

All those can't be changed to use `Future` API seamlessly. So to get a normal,
non-blocking interface based on Future's and coroutines a new library needed to
be written.


API differences and rationale
=============================

``aiokafka`` has some differences in API design. While the **Producer** is
mostly the same, **Consumer** has some significant differences, that we want
to talk about.


Consumer has no `poll()` method
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In ``kafka-python`` ``KafkaConsumer.poll()`` is a blocking call that performs
not only message fetching, but also:

  * Socket polling using `epoll`, `kqueue` or other available API of your OS.
  * Ensures liveliness of a Consumer Group
  * Does autocommit

This will never be a case where you own the IO loop, at least not with socket
polling. To avoid misunderstandings as to why do those methods behave in a
different way :ref:`aiokafka-consumer` exposes this interface under the name
``getmany()`` with some other differences described below.


Rebalances are happening in the background
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In original Kafka Java Client before 0.10.1 heartbeats were only sent if
``poll()`` was called. This lead to a lot of issues (reasons for `KIP-41`_ and
`KIP-62`_ proposals) and workarounds using `pause()` and `poll(0)` for
heartbeats. After Java client and kafka-python also changed the behaviour to
a background Thread sending, that mitigated most issues.

``aiokafka`` delegates heartbeating to a background *Task* and will send
heartbeats to Coordinator as long as the *event loop* is running. This
behaviour is very similar to Java client, with the exception of no heartbeats
on long CPU bound methods.

But ``aiokafka`` also performs group rebalancing in the same background Task. This
means, that the processing time between ``getmany`` calls actually does not
affect rebalancing. ``KIP-62`` proposed to provide ``max.poll.interval.ms`` as
the configuration for both *rebalance timeout* and *consumer processing
timeout*. In ``aiokafka`` it does not make much sense, as those 2 are not
related, so we added both configurations (``rebalance_timeout_ms`` and
``max_poll_interval_ms``).
It is quite critical to provide 
:ref:`ConsumerRebalanceListener <consumer-rebalance-listener>` if you need
to control rebalance start and end moments. In that case set the
``rebalance_timeout_ms`` to the maximum time your application can spend
waiting in the callback. If your callback waits for the last ``getmany`` result to
be processed, it is safe to set this value to ``max_poll_interval_ms``, same
as in Java client.


Prefetching is more sophisticated
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In Kafka Java Client and ``kafka-python`` the prefetching is very simple, as
it only performs prefetches:
 
  * in ``poll()`` call if we don't have enough data stored to satisfy another
    ``poll()``
  * in the *iterator* interface if we have processed *nearly* all data.

A very simplified version would be:

.. code:: python

    def poll():
        max_records = self.config['max_poll_records']
        records = consumer.fethed_records(max_records)
        if not consumer.has_enough_records(max_records)
            consumer.send_fetches()  # prefetch another batch
        return records

This works great for throughput as the algorithm is simple and we pipeline
IO task with record processing.

But it does not perform as great in case of **semantic partitioning**, where
you may have per-partition processing. In this case latency will be bound to
the time of processing of data in all topics.

Which is why ``aiokafka`` tries to do prefetches **per partition**. For
example, if we processed all data pending for a partition in *iterator*
interface, ``aiokafka`` will *try* to prefetch new data right away. The same
interface could be built on top of ``kafka-python``'s *pause* API, but
would require `a lot of code`_. 

.. note::
    
    Using ``getmany()`` without specifying partitions will result in the same
    prefetch behaviour as using ``poll()``

