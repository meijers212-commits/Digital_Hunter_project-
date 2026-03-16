from shaerd.logger.logger import log_event
import os

class ReraderConfig:
    
    def __init__(self, log_event: log_event):

        self.log_event = log_event

        self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
        self.consumer_topics = os.getenv("consumer_topics").split(",")
        self.producer_topic = os.getenv("producer_topic")
        self.client_id = os.getenv("client_id").split(",")
        self.sql_host = os.getenv("sql_host")
        self.sql_user = os.getenv("sql_user")
        self.sql_password = os.getenv("MYSQL_ROOT_PASSWORD")
        self.sql_database = os.getenv("MYSQL_DATABASE")

        self.consumer_config = {
        "bootstrap.servers": self.bootstrap_servers,
        "group.id": "order-tracker",
        "auto.offset.reset": "earliest"
        }

        self.validation()

    def validation(self):

        necessary_variables = {
            "bootstrap_servers": self.bootstrap_servers,
            "consumer_topic": self.consumer_topics,
            "producer_topic": self.producer_topic,
            "client_id": self.client_id,
            "sql_host": self.sql_host,
            "sql_user": self.sql_user,
            "sql_password": self.sql_password,
            "sql_database": self.sql_database,
        }

        missing = []

        for name , value in necessary_variables.items():
            if value is None:
                missing.append(name)

        if missing:
            self.log_event(level="exception", message="Required environment variables are missing.")
            raise Exception("Required environment variables are missing.")

        else:
            self.log_event(level="info", message="All variables found and loaded.")