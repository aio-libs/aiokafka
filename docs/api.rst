.. _api-doc:

API Documentation
=================

.. _aiokafka-producer:

AIOKafkaProducer class
----------------------

.. autoclass:: aiokafka.AIOKafkaProducer
    :members:


.. _aiokafka-consumer:

AIOKafkaConsumer class
----------------------

.. autoclass:: aiokafka.AIOKafkaConsumer
    :members:

Helpers
-------

.. _helpers:

.. automodule:: aiokafka.helpers
    :members:

.. _ssl_auth:

Abstracts
---------

.. _consumer-rebalance-listener:

.. autoclass:: aiokafka.ConsumerRebalanceListener
    :members:
    :exclude-members: on_partitions_revoked, on_partitions_assigned

    .. autocomethod:: aiokafka.ConsumerRebalanceListener.on_partitions_revoked()

    .. autocomethod:: aiokafka.ConsumerRebalanceListener.on_partitions_assigned()

SSL Authentication
------------------

Security is not an easy thing, at least when you want to do it right. Before
diving in on how to setup `aiokafka` to work with SSL, make sure there is
a need for SSL Authentication and go through the
`official documentation <http://kafka.apache.org/documentation.html#security_ssl>`_
for SSL support in Kafka itself.

`aiokafka` provides only ``ssl_context`` as a parameter for Consumer and
Producer classes. This is done intentionally, as it is recommended that you
read through the
`python ssl documentation <https://docs.python.org/3/library/ssl.html#security-considerations>`_
to have some understanding on the topic. Although if you know what you are
doing, there is a simple helper function `aiokafka.helpers.create_ssl_context`_,
that will create an ``ssl.SSLContext`` based on similar params to `kafka-python`.

A few notes on Kafka's SSL store types. Java uses **JKS** store type, that
contains normal certificates, same as ones OpenSSL (and Python, as it's based
on OpenSSL) uses, but encodes them into a single, encrypted file, protected by
another password. Just look the internet on how to extract `CARoot`,
`Certificate` and `Key` from JKS store.

See also the :ref:`ssl_example` example.


SASL Authentication
-------------------

As of version 0.5.1 aiokafka supports SASL authentication using both PLAIN and
GSSAPI sasl methods. Be sure to install ``gssapi`` python module to use GSSAPI.
Please consult the `official documentation <http://kafka.apache.org/documentation.html#security_sasl>`_
for setup instructions on Broker side. Client configuration is pretty much the
same as JAVA's, consult the ``sasl_*`` options in Consumer and Producer API
Referense for more details.


Error handling
--------------

Both consumer and producer can raise exceptions that inherit from the
`aiokafka.errors.KafkaError` class.

Exception handling example:


.. code:: python

        from aiokafka.errors import KafkaError, KafkaTimeoutError
        # ...
        try:
            send_future = await producer.send('foobar', b'test data')
            response = await send_future  #  wait until message is produced
        except KafkaTimeourError:
            print("produce timeout... maybe we want to resend data again?")
        except KafkaError as err:
            print("some kafka error on produce: {}".format(err))


Consumer errors
^^^^^^^^^^^^^^^

Consumer's ``async for`` and ``getone``/``getmany`` interfaces will handle those
differently. Possible consumer errors include:

    * ``TopicAuthorizationFailedError`` - topic requires authorization.
      Always raised
    * ``OffsetOutOfRangeError`` - if you don't specify `auto_offset_reset` policy
      and started cosumption from not valid offset. Always raised
    * ``RecordTooLargeError`` - broker has a *MessageSet* larger than
      `max_partition_fetch_bytes`. **async for** - log error, **get*** will
      raise it.
    * ``InvalidMessageError`` - CRC check on MessageSet failed due to connection
      failure or bug. Always raised. Changed in version ``0.5.0``, before we
      ignored this error in ``async for``.
