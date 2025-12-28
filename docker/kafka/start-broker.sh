#!/usr/bin/env bash


echo "
broker.id=${BROKER_ID:-0}
process.roles=broker,controller
listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://$(hostname -i):9092
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
controller.quorum.voters=${CONTROLLER_QUORUM_VOTERS}
controller.listener.names=CONTROLLER
offsets.topic.replication.factor=${OFFSETS_REPLICATIONS:-1}
" > config/runtime.properties

cat config/base-server.properties config/runtime.properties > config/server.properties

if [ ! -e "/tmp/kafka-logs/meta.properties" ]; then
  kafka-storage.sh format --config config/server.properties --cluster-id "YPKJRKEhT06jEqGlBQar5A"
fi

exec kafka-server-start.sh config/server.properties
