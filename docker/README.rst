Kafka Docker Builder
====================

Basic helper script to build multiversion docker containers with Kafka service.


Usage
-----

Edit `config.yml` to set up an image name and required versions. Images will have tags `{scala_version}_{kafka_version}` when built.

Run the builder::

    python build.py build


If you want to also push images to dockerhub:

    python build.py build push

