aiokafka
========
.. image:: https://travis-ci.org/aio-libs/aiokafka.svg?branch=master
    :target: https://travis-ci.org/aio-libs/aiokafka
    :alt: |Build status|
.. image:: https://coveralls.io/repos/aio-libs/aiokafka/badge.png?branch=master
    :target: https://coveralls.io/r/aio-libs/aiokafka?branch=master
    :alt: |Coverage|

asyncio client for kafka

Running tests
-------------

Docker is required to run tests. See https://docs.docker.com/engine/installation for installation notes.

Setting up tests requirements (assuming you're within virtualenv on ubuntu 14.04+)::

    sudo apt-get install -y libsnappy-dev && pip install flake8 pytest pytest-cov pytest-catchlog docker-py python-snappy coveralls .

Running tests::

    make cov

To run tests with a specific version of Kafka (default one is 0.9.0.1) use KAFKA_VERSION variable::

    make cov KAFKA_VERSION=0.8.2.1
