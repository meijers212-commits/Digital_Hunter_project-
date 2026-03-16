from shaerd.kafka.consumer import KafkaConsumer
from reader_config import ReraderConfig
from shaerd.logger.logger import log_event
from shaerd.kafka.producer import KafkaProducer
from logic import Logic
from shaerd.db.sql import Init_db_and_connection

config = ReraderConfig(log_event=log_event)

cons = KafkaConsumer(
    log_event=log_event,
    consumer_config=config.consumer_config,
    consumer_topic=config.consumer_topics,
)

consumer = cons.get_consumer()

producer = KafkaProducer(
    log_event=log_event,
    bootstrap_service=config.bootstrap_servers,
    producer_topic=config.producer_topic,
    client_id=config.client_id)

sql = Init_db_and_connection(
    log_event=log_event,
    host=config.sql_host,
    user=config.sql_user,
    password=config.sql_password,
    db_name=config.sql_database
    )

sql_conn = sql.connection


try:
    sql.create_db_and_table()

except Exception as e:
    log_event(level="error",message="cant create db and table", extra_info=e)

try:
    while True:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                print("non")
                continue

            if msg.error():
                print("error")
                log_event(level="error",message=f"Error: , {msg.error()}")
                continue
            
            try:

                value = msg.value().decode("utf-8")

                Logic.process_message(
                    validate_schema=Logic.validate_schema,
                    save_to_db=Logic.save_to_db,
                    log_event=log_event,
                    db_connection=sql_conn,
                    db_name=config.sql_database,
                    producer=producer,
                    msg_value=value,
                    topic=msg.topic()
                )

            except Exception as e:
                log_event(level="eroor",message="json is not good")
                continue

        except Exception as e:
            log_event(level="error",message=e)

except KeyboardInterrupt:
    log_event(level="info",message="🔴 Stopping consumer")

finally:
    consumer.close()
