#!/bin/sh

OPTIONS=""
PATH="$HOME/bin:$PATH"

# Configure the default number of log partitions per topic
if [ ! -z "$NUM_PARTITIONS" ]; then
    echo "default number of partition: $NUM_PARTITIONS"
    OPTIONS="$OPTIONS --override num.partitions=$NUM_PARTITIONS"
fi

# Set the external host and port
echo "advertised host: $ADVERTISED_HOST"
echo "advertised port: $ADVERTISED_PORT"

LISTENERS="PLAINTEXT://:$ADVERTISED_PORT"
ADVERTISED_LISTENERS="PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_PORT"

if [ ! -z "$ADVERTISED_SSL_PORT" ]; then
    echo "advertised ssl port: $ADVERTISED_SSL_PORT"

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
    OPTIONS="$OPTIONS --override ssl.endpoint.identification.algorithm="

    LISTENERS="$LISTENERS,SSL://:$ADVERTISED_SSL_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SSL://$ADVERTISED_HOST:$ADVERTISED_SSL_PORT"
fi

if [ ! -z "$SASL_MECHANISMS" ]; then
    echo "sasl mechanisms: $SASL_MECHANISMS"
    echo "advertised sasl plaintext port: $ADVERTISED_SASL_PLAINTEXT_PORT"
    echo "advertised sasl ssl port: $ADVERTISED_SASL_SSL_PORT"

    OPTIONS="$OPTIONS --override sasl.enabled.mechanisms=$SASL_MECHANISMS"
    OPTIONS="$OPTIONS --override authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer"
    OPTIONS="$OPTIONS --override allow.everyone.if.no.acl.found=true"
    export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/kafka_server_jaas.conf"
    
    LISTENERS="$LISTENERS,SASL_PLAINTEXT://:$ADVERTISED_SASL_PLAINTEXT_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SASL_PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_SASL_PLAINTEXT_PORT"

    LISTENERS="$LISTENERS,SASL_SSL://:$ADVERTISED_SASL_SSL_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SASL_SSL://$ADVERTISED_HOST:$ADVERTISED_SASL_SSL_PORT"
fi

# Enable auto creation of topics
OPTIONS="$OPTIONS --override auto.create.topics.enable=true"
OPTIONS="$OPTIONS --override listeners=$LISTENERS"
OPTIONS="$OPTIONS --override advertised.listeners=$ADVERTISED_LISTENERS"
OPTIONS="$OPTIONS --override super.users=User:admin"


# Run Kafka
echo "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS"

exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS
