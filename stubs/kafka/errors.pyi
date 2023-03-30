class KafkaError(RuntimeError):
    retriable: bool
    invalid_metadata: bool
