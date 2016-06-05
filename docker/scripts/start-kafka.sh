#!/bin/sh

OPTIONS=""

# Set the external host and port
if [ ! -z "$ADVERTISED_HOST" ]; then
    echo "advertised host: $ADVERTISED_HOST"
    OPTIONS="$OPTIONS --override advertised.host.name=$ADVERTISED_HOST"
fi
if [ ! -z "$ADVERTISED_PORT" ]; then
    echo "advertised port: $ADVERTISED_PORT"
    OPTIONS="$OPTIONS --override advertised.port=$ADVERTISED_PORT"
fi

PATH="$HOME/bin:$PATH"

# Configure the default number of log partitions per topic
if [ ! -z "$NUM_PARTITIONS" ]; then
    echo "default number of partition: $NUM_PARTITIONS"
    OPTIONS="$OPTIONS --override num.partitions=$NUM_PARTITIONS"
fi

# Enable/disable auto creation of topics
OPTIONS="$OPTIONS --override auto.create.topics.enable=true"

# Run Kafka
echo "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS"

$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS
