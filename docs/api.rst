
API Documentation
=================

AIOKafkaProducer class
----------------------

.. autoclass:: aiokafka.AIOKafkaProducer
    :members:

AIOKafkaConsumer class
----------------------

.. autoclass:: aiokafka.AIOKafkaConsumer
    :members:

Errors handling
---------------

Both consumer and producer can raise exceptions that inherit from the `kafka.common.KafkaError` class
and declared in `kafka.common` module.

Example of exceptions catching:


.. code:: python

        from kafka.common import KafkaError, KafkaTimeoutError
        # ... 
        try:
            send_future = yield from producer.send('foobar', b'test data')
            response = yield from send_future  #  wait until message is produced
        except KafkaTimeourError:
            print("produce timeout... maybe we want to resend data again?")
        except KafkaError as err:
            print("some kafka error on produce: {}".format(err))
