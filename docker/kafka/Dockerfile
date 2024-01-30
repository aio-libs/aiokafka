FROM ubuntu:22.04

RUN apt-get update && \
	apt-get install -y --no-install-recommends default-jre wget && \
  rm -rf /var/lib/apt/lists/*

ENV PATH="/opt/kafka/bin:${PATH}"
WORKDIR /opt/kafka

COPY start-broker.sh /opt/kafka/bin/

ARG SCALA_VERSION=2.13
ARG KAFKA_VERSION=2.8.1

RUN wget -q -O kafka.tgz "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
  && tar xfvz kafka.tgz --strip 1 \
  && rm -rf kafka.tgz site-docs
