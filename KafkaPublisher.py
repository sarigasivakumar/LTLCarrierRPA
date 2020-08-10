
from kafka import KafkaProducer, errors
from json import dumps
import sys
class KafkaPublisher:

    def __init__(self, server=['10.138.0.2:9092','10.138.0.3:9092','10.138.0.4:9092'], topic='RAW_AIR_CARGO_RPA_DEV',retries=5):

        self.bootstrap_server = server
        self.kafka_topic = topic
        self.retries = retries



        while True:

            try:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap_server, retries=self.retries,
                                    value_serializer=lambda x: dumps(x).encode('utf-8'), api_version=(1, 3, 5),
                                    request_timeout_ms=1000000, api_version_auto_timeout_ms=1000000)
                break
            except errors.BrokerNotAvailableError as e:
                print(e," retrying")

    def publish(self, data):
        self.producer.send(self.kafka_topic, data)
        self.producer.flush()

    def close(self, timeout=60,):

        try:
            self.producer.close(timeout)
        except errors.KafkaTimeoutError: # failed to flushed buffered record
            return -1
        return 0


    def bootstrap_server(self):
        return self.bootstrap_server

    def kafka_topic(self):
        return self.kafka_topic

    def retries(self):
        return self.retries
