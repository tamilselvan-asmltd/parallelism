# Kafka Producer and PyFlink Parallelism Example

This project contains two main components: a Kafka producer script and a PyFlink parallelism script. The Kafka producer generates random lists of integers and sends them to a Kafka topic, while the PyFlink script consumes the messages from the Kafka topic and performs sum and average calculations in parallel.

## Files

1. **list_input_producer.py**
   - This script generates random lists of integers and produces them to a Kafka topic named `pwdtopic`.

2. **parallelism.py**
   - This script sets up a PyFlink job to consume messages from the `pwdtopic` Kafka topic, calculate the sum and average of the integers, and print the results.
## Input from kafka producer
```
>[1,2,3,4]
```
## Output

The output of the PyFlink job will look like this:
```
Final Average: 1.0
Final Average: 1.5
Final Average: 2.0
Final Average: 2.5
Final Sum: 1
Final Sum: 3
Final Sum: 6
Final Sum: 10
```

## Requirements

- Kafka
- Python
- Confluent Kafka Python client
- PyFlink
- flink-sql-connector-kafka-1.17.1.jar

## Setup and Execution

1. **Kafka Setup**
   - Ensure Kafka is running on your local machine.

2. **Producer Setup**
   - Install the Confluent Kafka Python client:
     ```
     pip install confluent_kafka
     ```
   - Run the Kafka producer script:
     ```
     python list_input_producer.py
     ```

3. **Flink Setup**
   - Download and place the `flink-sql-connector-kafka-1.17.1.jar` file in your desired directory.
   - Install PyFlink:
     ```
     pip install apache-flink
     ```
   - Run the Flink parallelism script:
     ```
     python parallelism.py
     ```

Ensure you have all dependencies installed and properly configured to successfully run the scripts and observe the output.
