from aiokafka.consumer.consumer import AIOKafkaConsumer


def test_consumer_stores_client_rack():
    # When initialized with client_rack, it should store the attribute
    rack = "rack-test"
    consumer = AIOKafkaConsumer(client_rack=rack)
    assert hasattr(consumer, "_client_rack"), "Consumer missing _client_rack attribute"
    assert consumer._client_rack == rack


def test_consumer_default_client_rack_none():
    # If not provided, default should be None
    consumer = AIOKafkaConsumer()
    assert hasattr(consumer, "_client_rack"), "Consumer missing _client_rack attribute"
    assert consumer._client_rack is None
