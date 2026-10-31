.. _listeners:

Event listeners
===============

.. warning::

    The listener API is experimental and may change without a
    deprecation period. The existing ``ConsumerRebalanceListener`` API remains
    stable.

Listeners receive lifecycle events for observability or other application
actions. They are not tied to a metrics backend. The public interfaces live in
``aiokafka.listeners`` and are also exported from ``aiokafka``:

* ``ConnectionListener`` provides connection opened, closed, and failed events.
* ``ProducerListener(ConnectionListener)`` adds batch dispatch, completion,
  failure, and buffer-wait events.

Consumers use ``ConnectionListener`` for connection events. Connection details
and enums live in ``aiokafka.structs`` and are also exported from ``aiokafka``.

Each method has a no-op default. Override only the events you need. Pass an
iterable of instances through ``listeners=`` on a producer, consumer, or
low-level client. Registration is copied at construction, preserving iterable
order; modifying the original collection does not change registration. Duplicate
instances in that iterable raise ``ValueError``. A low-level client delivers
only connection events; producer batch events are delivered by the producer.

.. code-block:: python

    from aiokafka import AIOKafkaProducer, ProducerListener


    class ApplicationEvents(ProducerListener):
        def on_connection_failed(self, connection, error):
            print("Connection failed:", connection.host, error)

        def on_batch_completed(self, **event):
            print("Batch completed:", event["topic"], event["record_count"])


    producer = AIOKafkaProducer(listeners=[ApplicationEvents()])

Dispatch contracts
------------------

The interfaces share registration and optional overrides. Delivery follows the
contract of each event:

.. list-table::
    :header-rows: 1
    :widths: 25 35 40

    * - Events
      - Execution
      - Effect on client progress
    * - Connections
      - Background tasks; synchronous or awaitable callbacks
      - Requests do not wait. Delivery has a capacity limit and shutdown grace.
    * - Producer batches and buffer waits
      - Synchronous callbacks on the producer path
      - Keep callbacks fast; use an application queue for asynchronous work.

For an event, listeners run in registration order. Exceptions are logged and
the next listener is called; cancellation of a running asynchronous dispatch
propagates. Connection events also preserve order per connection. A slow
listener delays later listeners for that connection, but other connections
have independent tasks. See :ref:`connection-lifecycle` for overload and
shutdown behavior and :ref:`metrics` for batch event arguments.

With the default ``listeners=()``, no default listener objects or connection
dispatcher are created. Unoverridden methods are filtered at registration, so
even a listener using only batch events creates no connection observer work.
Producer dispatch iterates only the overridden callbacks, with an empty tuple
for unused events.

Migration from producer metrics
-------------------------------

Replace imports of the experimental ``ProducerMetricsCollector`` with
``ProducerListener`` from ``aiokafka`` or ``aiokafka.listeners``, and replace
``metrics_collector=collector`` with ``listeners=[collector]``. The old class,
constructor argument, and ``aiokafka.metrics`` module have been removed.
Batch callback arguments and synchronous execution are unchanged.

Consumer rebalances
-------------------

Rebalance events continue to use ``ConsumerRebalanceListener`` and
``consumer.subscribe(listener=...)``. Their stable API, including support for
coroutine callbacks and coordinator ordering guarantees, is unchanged.
Constructor ``listeners`` do not register rebalance callbacks. An application
object implementing both interfaces must be registered through each entry
point to receive both kinds of event.
