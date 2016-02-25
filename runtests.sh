#!/bin/bash

DOCKERIP=`ip -f inet addr show docker0 | grep "inet 172" | awk '{print \$2}' | grep -E -o "([0-9]{1,3}[\.]){3}[0-9]{1,3}"`
KAFKA_PORT=19092
echo "Running docker container with Kafka and ZooKeeper ($DOCKER_IMAGE_NAME)"
CID=`docker run -p $DOCKERIP::2181 -p $DOCKERIP:$KAFKA_PORT:9092 --env ADVERTISED_HOST=$DOCKERIP --env ADVERTISED_PORT=$KAFKA_PORT --env NUM_PARTITIONS=2 -d $DOCKER_IMAGE_NAME`
trap "echo \"Trapped. removing docker container...\"; docker stop $CID; docker rm $CID; exit 1" HUP INT TERM

echo "Waiting for Kafka to become available..."
while true; do
    nc -zq 1 $DOCKERIP $KAFKA_PORT && break
    sleep .1
done

KAFKA_HOST=$DOCKERIP KAFKA_PORT=$KAFKA_PORT nosetests -s $FLAGS $EXTRA_NOSE_ARGS ./tests/ --ignore-files test_consumer.py
RESULT=$?
echo "Removing docker container..."
docker stop $CID
docker rm $CID
exit $RESULT
