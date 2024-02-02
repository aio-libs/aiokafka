FROM ubuntu:22.04

RUN apt-get update && \
	apt-get install -y --no-install-recommends default-jre wget && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/kafka/bin:${PATH}"
WORKDIR /opt/kafka

# API like CreateTopics are redirected automatically to the controller starting to 2.8.0
# https://issues.apache.org/jira/browse/KAFKA-10181
# To reproduce the error, we must use a previous version of kafka
ARG SCALA_VERSION=2.13
ARG KAFKA_VERSION=3.3.0

RUN wget -q -O kafka.tgz "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
  && tar xfvz kafka.tgz --strip 1 \
  && rm -rf kafka.tgz site-docs

COPY start-broker.sh /opt/kafka/bin/
COPY base-server.properties /opt/kafka/config