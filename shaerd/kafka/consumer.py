from confluent_kafka import Consumer
import time
from shaerd.logger.logger import log_event

class KafkaConsumer:
    def __init__(self, log_event: log_event, consumer_config: dict, consumer_topic: list):

        self.log_event = log_event

        try:

            self.consumer = Consumer(consumer_config)
            self.log_event(level="info", message="consumer started")

        except Exception as e:
            self.log_event(level="exception", message=f"Unable to activate the consumer, Error:{e}")
            raise Exception(e)

        try:
            self.consumer.subscribe(consumer_topic)
            self.log_event(level="info", message=f"🟢 Consumer is running and subscribed to topic: {consumer_topic}")
        except Exception as e:

            self.log_event(level="exception", message=e)
            raise Exception(e)
        
        # try:

        #     while True:

        #         topics = self.consumer.list_topics(timeout=5)

        #         if consumer_topic in topics.topics:

        #             self.consumer.subscribe(consumer_topic)
        #             self.log_event(level="info", message=f"🟢 Consumer is running and subscribed to topic: {consumer_topic}")
        #             break

        #         else:
        #             self.log_event(level="info", message=f"topic: {consumer_topic} not found trying again")
        #             time.sleep(3)

        # except Exception as e:
        #     self.log_event(level="exception", message=e)
        #     raise Exception(e)


    def get_consumer(self) -> Consumer:
        return self.consumer
    
    def close_consumer(self) -> None:
        self.consumer.close()
        self.log_event(level="info", message="consumer closed ..")
