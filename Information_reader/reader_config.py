from shaerd.logger.logger import log_event
import os

class ReraderConfig:
    
    def __init__(self, log_event: log_event):

        self.bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS")
        self.consumer_topic = os.getenv("consumer_topic")
        self.consumer_group_id = os.getenv("consumer_group_id")

        self.validation()

    def validation(self):

        necessary_variables = {
            "bootstrap_servers": self.bootstrap_servers,
            "consumer_topic": self.consumer_topic,
            "consumer_group_id": self.consumer_group_id
        }

        missing = []

        for name , value in necessary_variables.items():
            if value is None or value.strip() == "":
                missing.append(name)

        if missing:
            self.log_event(level="exception", message="Required environment variables are missing.")
            raise Exception("Required environment variables are missing.")

        else:
            self.log_event(level="info", message="All variables found and loaded.")

