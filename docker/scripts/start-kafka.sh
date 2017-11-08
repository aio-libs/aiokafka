#!/bin/sh

OPTIONS=""
PATH="$HOME/bin:$PATH"

# Configure the default number of log partitions per topic
if [ ! -z "$NUM_PARTITIONS" ]; then
    echo "default number of partition: $NUM_PARTITIONS"
    OPTIONS="$OPTIONS --override num.partitions=$NUM_PARTITIONS"
fi


# Configure the default number of log partitions per topic
if [ ! -z "$ADVERTISED_SSL_PORT" ]; then
    # Set the external host and port
    echo "advertised host: $ADVERTISED_HOST"
    echo "advertised port: $ADVERTISED_PORT"
    echo "advertised ssl port: $ADVERTISED_SSL_PORT"
    OPTIONS="$OPTIONS --override advertised.listeners=PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_PORT,SSL://$ADVERTISED_HOST:$ADVERTISED_SSL_PORT"

    # SSL options
    OPTIONS="$OPTIONS --override ssl.protocol=TLS"
    OPTIONS="$OPTIONS --override ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1"
    OPTIONS="$OPTIONS --override ssl.keystore.type=JKS"
    OPTIONS="$OPTIONS --override ssl.keystore.location=/ssl_cert/br_server.keystore.jks"
    OPTIONS="$OPTIONS --override ssl.keystore.password=abcdefgh"
    OPTIONS="$OPTIONS --override ssl.key.password=abcdefgh"
    OPTIONS="$OPTIONS --override ssl.truststore.type=JKS"
    OPTIONS="$OPTIONS --override ssl.truststore.location=/ssl_cert/br_server.truststore.jks"
    OPTIONS="$OPTIONS --override ssl.truststore.password=abcdefgh"
    OPTIONS="$OPTIONS --override ssl.client.auth=required"
    OPTIONS="$OPTIONS --override security.inter.broker.protocol=SSL"
    OPTIONS="$OPTIONS --override listeners=PLAINTEXT://:$ADVERTISED_PORT,SSL://:$ADVERTISED_SSL_PORT"

else

    # Set the external host and port
    echo "advertised host: $ADVERTISED_HOST"
    echo "advertised port: $ADVERTISED_PORT"
    OPTIONS="$OPTIONS --override advertised.listeners=PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_PORT"    
    OPTIONS="$OPTIONS --override listeners=PLAINTEXT://:$ADVERTISED_PORT"

fi

# Enable auto creation of topics
OPTIONS="$OPTIONS --override auto.create.topics.enable=true"

# Run Kafka
echo "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS"

exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS
