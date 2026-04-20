Add rack-aware fetching from the closest in-sync replica (KIP-392) via the new ``client_rack`` option on :class:`AIOKafkaConsumer`. When set and the brokers support ``FetchRequest v11`` (Kafka 2.4+) with a ``replica.selector.class`` configured, the consumer will fetch from a same-rack follower instead of the partition leader, reducing cross-AZ traffic and tail latency.

