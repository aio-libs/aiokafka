#!/bin/bash

OPTIONS=()
PATH="$HOME/bin:$PATH"

# Configure the default number of log partitions per topic
if [ ! -z "$NUM_PARTITIONS" ]; then
    echo "default number of partition: $NUM_PARTITIONS"
    OPTIONS+=("--override" "num.partitions=$NUM_PARTITIONS")
fi

# Set the external host and port
echo "advertised host: $ADVERTISED_HOST"
echo "advertised port: $ADVERTISED_PORT"

LISTENERS="PLAINTEXT://:$ADVERTISED_PORT"
ADVERTISED_LISTENERS="PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_PORT"

if [[ ! -z "$ADVERTISED_SSL_PORT" ]]; then
    echo "advertised ssl port: $ADVERTISED_SSL_PORT"

    # SSL options
    OPTIONS+=("--override" "ssl.protocol=TLS")
    OPTIONS+=("--override" "ssl.enabled.protocols=TLSv1.2,TLSv1.1,TLSv1")
    OPTIONS+=("--override" "ssl.keystore.type=JKS")
    OPTIONS+=("--override" "ssl.keystore.location=/ssl_cert/br_server.keystore.jks")
    OPTIONS+=("--override" "ssl.keystore.password=abcdefgh")
    OPTIONS+=("--override" "ssl.key.password=abcdefgh")
    OPTIONS+=("--override" "ssl.truststore.type=JKS")
    OPTIONS+=("--override" "ssl.truststore.location=/ssl_cert/br_server.truststore.jks")
    OPTIONS+=("--override" "ssl.truststore.password=abcdefgh")
    OPTIONS+=("--override" "ssl.client.auth=required")
    OPTIONS+=("--override" "security.inter.broker.protocol=SSL")
    OPTIONS+=("--override" "ssl.endpoint.identification.algorithm=")

    LISTENERS="$LISTENERS,SSL://:$ADVERTISED_SSL_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SSL://$ADVERTISED_HOST:$ADVERTISED_SSL_PORT"
fi

if [[ ! -z "$SASL_MECHANISMS" ]]; then
    echo "sasl mechanisms: $SASL_MECHANISMS"
    echo "advertised sasl plaintext port: $ADVERTISED_SASL_PLAINTEXT_PORT"
    echo "advertised sasl ssl port: $ADVERTISED_SASL_SSL_PORT"

    OPTIONS+=("--override" "sasl.enabled.mechanisms=$SASL_MECHANISMS")
    OPTIONS+=("--override" "sasl.kerberos.service.name=kafka")
    OPTIONS+=("--override" "authorizer.class.name=kafka.security.auth.SimpleAclAuthorizer")
    OPTIONS+=("--override" "allow.everyone.if.no.acl.found=true")
    export KAFKA_OPTS="-Djava.security.auth.login.config=/etc/kafka/$SASL_JAAS_FILE"

    # OAUTHBEARER configuration is incompatible with other SASL configurations present in JAAS file
    if [[ "$SASL_MECHANISMS" == "OAUTHBEARER" ]]; then
        OPTIONS+=("--override" "listener.name.sasl_plaintext.oauthbearer.sasl.jaas.config=
           org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
           unsecuredLoginStringClaim_sub=\"producer\"
           unsecuredValidatorAllowableClockSkewMs=\"3000\";"
        )
        OPTIONS+=("--override" "listener.name.sasl_ssl.oauthbearer.sasl.jaas.config=
           org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required
           unsecuredLoginStringClaim_sub=\"consumer\"
           unsecuredValidatorAllowableClockSkewMs=\"3000\";"
        )
        OPTIONS+=("--override" "listener.name.sasl_plaintext.oauthbearer.connections.max.reauth.ms=3600000")
        OPTIONS+=("--override" "listener.name.sasl_ssl.oauthbearer.connections.max.reauth.ms=3600000")
    fi

    LISTENERS="$LISTENERS,SASL_PLAINTEXT://:$ADVERTISED_SASL_PLAINTEXT_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SASL_PLAINTEXT://$ADVERTISED_HOST:$ADVERTISED_SASL_PLAINTEXT_PORT"

    LISTENERS="$LISTENERS,SASL_SSL://:$ADVERTISED_SASL_SSL_PORT"
    ADVERTISED_LISTENERS="$ADVERTISED_LISTENERS,SASL_SSL://$ADVERTISED_HOST:$ADVERTISED_SASL_SSL_PORT"
fi

# Enable auto creation of topics
OPTIONS+=("--override" "auto.create.topics.enable=true")
OPTIONS+=("--override" "listeners=$LISTENERS")
OPTIONS+=("--override" "advertised.listeners=$ADVERTISED_LISTENERS")
OPTIONS+=("--override" "super.users=User:admin")


# Run Kafka
echo "$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties $OPTIONS"

exec $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties "${OPTIONS[@]}"
