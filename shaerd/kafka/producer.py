from confluent_kafka import Producer
import json

class KafkaProducer:

    def __init__(self, log_event, bootstrap_service, producer_topic, client_id):

        self.publisher_topic = producer_topic

        self.log_event = log_event

        self.conf = {
            "bootstrap.servers": bootstrap_service,
            "client.id": client_id,
        }

        try:

            self.producer = Producer(self.conf)
            self.log_event(level="info", message="Publisher started successfully...")

        except Exception as e:
            self.log_event(level="exception", message=f"Publisher activation failed, Error: {e}")

    def acked(self, err, msg):
        if err is not None:
            self.log_event(level="info",message="Failed to deliver message: %s: %s" % (str(msg), str(err)))
        else:
            self.log_event(level="info",message="Message produced: %s" % (str(msg)))


    def publish(self, payload):

        self.producer.produce(
            self.publisher_topic,
            value=json.dumps(payload).encode("utf-8"),
            callback=self.acked,
        )

        self.producer.poll(1)

    def flush(self):
        self.producer.flush()