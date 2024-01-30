#!/usr/bin/env bash

exec kafka-server-start.sh config/server.properties \
  --override "broker.id=${BROKER_ID:-0}" \
  --override "zookeeper.connect=${ZOOKEEPER}" \
  --override "listeners=PLAINTEXT://:9092" \
  --override "advertised.listeners=PLAINTEXT://$(hostname -i):9092" \
  --override "listener.security.protocol.map=PLAINTEXT:PLAINTEXT" \
  --override "offsets.topic.replication.factor=${OFFSETS_REPLICATIONS:-1}"
