from shaerd.kafka.consumer import KafkaConsumer
from reader_config import ReraderConfig
from shaerd.logger.logger import log_event
import json

config = ReraderConfig()

cons = KafkaConsumer(
    log_event=log_event,
    consumer_config=config.consumer_config,
    consumer_topic=config.consumer_topics,
)

consumer = cons.get_consumer()

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log_event(level="error",message=f"❌ Error: , {msg.error()}")
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        
        topic = msg.topic()

        if topic == "Intel":
            print(f"topic: {topic}, msg:{msg}", "\n")
        
        if topic == "Attack":
            print(f"topic: {topic}, msg:{msg}", "\n")

        if topic == "Damage":
            print(f"topic: {topic}, msg:{msg}", "\n")

except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    consumer.close()
