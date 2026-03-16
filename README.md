# Digital Hunter 

## A system for managing and monitoring goals in real time

### Project description

```
The system consumes messages from three threads, validates the structure of the information that comes from a data cross-reference, and stores it in a SQL-based database.

```

### Handling errors and missing data

```
The system knows how to identify problematic data that does not match the required data or is missing critical information and forwards it to a server that knows how to handle this data.
```

### Architecture and structure

```
Receiving: The system receives the data from the appropriate topic.
```
```
Validation: The system checks for missing schemas and critical values ​​and handles JSONs that arrive invalid and sends them forward if necessary.
```
```
Processing and saving : The system processes the data, crosses it with what already exists in the database if it exists, and updates the required fields in the database.
```
### system components

```
shaerd: A system folder that contains the functions needed to connect to databases, create them, save them, and handle errors. A logger that saves the logs to the elasticsearch array.
```
```
Kafka: A folder that contains the consumer, which contains the logic responsible for receiving data from the correct topic, as well as the producer, whose job is to forward the problematic messages to the correct topic for further processing and ensuring proper continuation of system activity.
```
```
sql: Contains the functionality to create the database and the required tables, as well as a function to bind to the database to access data as needed.
```
```
logger: Contains the logger system for tracking errors and system messages and saving them in Elasticsearch.
```
```
Information_reader: A folder that contains the logic file that performs all the necessary operations and updates the database as needed and the rules that have been defined, as well as the 'process_message' function that can perform all the operations together by using the written logic, as well as the function required to calculate the distance 'haversine_km'
```
```
Information_reader: It also contains the reader_orchestrator file, which can read all the required functionality and bring it into full operation, and is the file that is run.
```
```
docker-compose.yaml: A file containing the system configuration for running and deploying it in the docker system.
```
## Running and operating the system

```
Note that the 'docker' system is installed on your computer in order to run the program - note that all required environment variables appear in the docker-compose.yaml file. Without this, the system will not be able to run and execute the following command via the terminal.
```
# Run command
```
docker compose up -d --build 
```
