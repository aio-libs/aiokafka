.. _listeners:

Producer event listeners
========================

.. warning::

    This API is experimental and may change without a deprecation period.

``ProducerListener`` receives batch dispatch, completion, failure, and
buffer-wait events. These events can be used for metrics or other application
actions. Import the interface from ``aiokafka`` or ``aiokafka.listeners`` and
register instances with ``AIOKafkaProducer(listeners=[...])``.

.. code-block:: python

    from aiokafka import AIOKafkaProducer, ProducerListener


    class ApplicationEvents(ProducerListener):
        def on_batch_completed(self, **event):
            print("Batch completed:", event["topic"], event["record_count"])


    producer = AIOKafkaProducer(listeners=[ApplicationEvents()])

Each method has a no-op default. Override only the events you need.
Registration is copied at construction, preserving iterable order; modifying
the original collection does not change registration. Passing the same instance
twice raises ``ValueError``.

Callbacks run synchronously in registration order. An exception is logged and
does not prevent the next listener from running. Keep callbacks fast and
non-blocking; use an application queue for asynchronous work. See :ref:`metrics`
for callback arguments and event boundaries.

With the default ``listeners=()``, no default listener objects are created.
Unoverridden methods are filtered at registration. Dispatch iterates only the
overridden callbacks, using an empty tuple for unused events.

Migration from producer metrics
-------------------------------

Replace imports of the experimental ``ProducerMetricsCollector`` with
``ProducerListener``, and replace ``metrics_collector=collector`` with
``listeners=[collector]``. The old class, constructor argument, and
``aiokafka.metrics`` module have been removed. Callback arguments and
synchronous execution are unchanged.
