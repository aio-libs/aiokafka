# Kafka and Zookeeper (borrowed from spotify/kafka)

FROM java:openjdk-8-jre

ENV DEBIAN_FRONTEND noninteractive
ARG SCALA_VERSION=2.11
ARG KAFKA_VERSION=0.9.0.1
ENV SCALA_VERSION $SCALA_VERSION
ENV KAFKA_VERSION $KAFKA_VERSION
ENV KAFKA_HOME /opt/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION"

# Install Kafka, Zookeeper and other needed things
RUN apt-get update && \
    apt-get install -y zookeeper wget supervisor dnsutils krb5-admin-server krb5-kdc && \
    rm -rf /var/lib/apt/lists/* && \
    apt-get clean && \
    wget -q http://apache.mirrors.spacedump.net/kafka/"$KAFKA_VERSION"/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -O /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz && \
    tar xfz /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz -C /opt && \
    rm /tmp/kafka_"$SCALA_VERSION"-"$KAFKA_VERSION".tgz

ADD scripts/start-kafka.sh /usr/bin/start-kafka.sh
ADD scripts/kafka_server_jaas.conf /etc/kafka/kafka_server_jaas.conf
ADD scripts/krb5.conf /etc/krb5.conf

# Supervisor config
ADD supervisor/kdc.conf supervisor/kafka.conf supervisor/zookeeper.conf /etc/supervisor/conf.d/

RUN kdb5_util create -r AIOLIBS -s -P aiokafka

RUN kadmin.local -q "add_principal -pw aiokafka client/localhost@AIOLIBS"
RUN kadmin.local -q "add_principal -randkey kafka/localhost@AIOLIBS"
RUN kadmin.local -q "ktadd -k /server.keytab kafka/localhost@AIOLIBS"

CMD ["supervisord", "-n"]
