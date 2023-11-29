.. _api-doc:

.. _gssapi: https://pypi.org/project/gssapi/

API Documentation
=================

.. _aiokafka-producer:

Producer class
--------------

.. autoclass:: aiokafka.AIOKafkaProducer
    :member-order: alphabetical
    :members:


Consumer class
--------------

.. autoclass:: aiokafka.AIOKafkaConsumer
    :member-order: alphabetical
    :members:


Helpers
-------

.. _helpers:

.. automodule:: aiokafka.helpers
    :member-order: alphabetical
    :members:

Abstracts
---------

.. autoclass:: aiokafka.abc.AbstractTokenProvider
    :members:

.. autoclass:: aiokafka.abc.ConsumerRebalanceListener
    :members:


.. _ssl_auth:

SSL Authentication
------------------

Security is not an easy thing, at least when you want to do it right. Before
diving in on how to setup `aiokafka` to work with SSL, make sure there is
a need for SSL Authentication and go through the
`official documentation <http://kafka.apache.org/documentation.html#security_ssl>`__
for SSL support in Kafka itself.

`aiokafka` provides only ``ssl_context`` as a parameter for Consumer and
Producer classes. This is done intentionally, as it is recommended that you
read through the
`Python ssl documentation <https://docs.python.org/3/library/ssl.html#security-considerations>`_
to have some understanding on the topic. Although if you know what you are
doing, there is a simple helper function :func:`aiokafka.helpers.create_ssl_context`,
that will create an :class:`ssl.SSLContext` based on similar params to `kafka-python`_.

A few notes on Kafka's SSL store types. Java uses **JKS** store type, that
contains normal certificates, same as ones OpenSSL (and Python, as it's based
on OpenSSL) uses, but encodes them into a single, encrypted file, protected by
another password. Just look the internet on how to extract `CARoot`,
`Certificate` and `Key` from JKS store.

See also the :ref:`ssl_example` example.


SASL Authentication
-------------------

As of version 0.5.1 aiokafka supports SASL authentication using both ``PLAIN``
and ``GSSAPI`` SASL methods. Be sure to install `gssapi`_ python module to use
``GSSAPI``.

Please consult the `official documentation <http://kafka.apache.org/documentation.html#security_sasl>`__
for setup instructions on Broker side. Client configuration is pretty much the
same as Java's, consult the ``sasl_*`` options in Consumer and Producer API
Reference for more details. See :class:`~aiokafka.abc.AbstractTokenProvider`.


Error handling
--------------

Both consumer and producer can raise exceptions that inherit from the
:exc:`aiokafka.errors.KafkaError` class.

Exception handling example:


.. code:: python

  from aiokafka.errors import KafkaError, KafkaTimeoutError
  # ...
  try:
      send_future = await producer.send('foobar', b'test data')
      response = await send_future  #  wait until message is produced
  except KafkaTimeoutError:
      print("produce timeout... maybe we want to resend data again?")
  except KafkaError as err:
      print("some kafka error on produce: {}".format(err))


Consumer errors
^^^^^^^^^^^^^^^

Consumer's ``async for`` and
:meth:`~.AIOKafkaConsumer.getone`/:meth:`~.AIOKafkaConsumer.getmany` interfaces
will handle those differently. Possible consumer errors include:

* :exc:`~aiokafka.errors.TopicAuthorizationFailedError` - topic requires authorization.
  Always raised
* :exc:`~aiokafka.errors.OffsetOutOfRangeError` - if you don't specify `auto_offset_reset` policy
  and started cosumption from not valid offset. Always raised
* :exc:`~aiokafka.errors.RecordTooLargeError` - broker has a *MessageSet* larger than
  `max_partition_fetch_bytes`. **async for** - log error, **get*** will
  raise it.
* :exc:`~aiokafka.errors.InvalidMessageError` - CRC check on MessageSet failed due to connection
  failure or bug. Always raised. Changed in version ``0.5.0``, before we
  ignored this error in ``async for``.



Other references
----------------

.. autoclass:: aiokafka.producer.message_accumulator.BatchBuilder
.. autoclass:: aiokafka.consumer.group_coordinator.GroupCoordinator
.. autoclass:: aiokafka.coordinator.assignors.roundrobin.RoundRobinPartitionAssignor


Errors
^^^^^^

.. automodule:: aiokafka.errors
    :member-order: alphabetical
    :ignore-module-all:
    :members:


Structs
^^^^^^^

.. automodule:: aiokafka.structs

.. autoclass:: aiokafka.structs.TopicPartition
    :members:

.. autoclass:: aiokafka.structs.RecordMetadata
    :member-order: alphabetical
    :members:

.. autoclass:: aiokafka.structs.ConsumerRecord
    :member-order: alphabetical
    :members:

.. autoclass:: aiokafka.structs.OffsetAndTimestamp
    :member-order: alphabetical
    :members:

.. py:class:: KT

    The type of a key.

.. py:class:: VT

    The type of a value.


Protocols
^^^^^^^^^

.. autoclass:: aiokafka.protocol.produce.ProduceRequest
    :member-order: alphabetical
    :members:
