
# Kafka Producer and PyFlink Stream Processing Example

This project demonstrates how to produce messages to a Kafka topic using a Python Kafka Producer and process these messages using PyFlink for parallel stream processing. The setup includes two main scripts:

1. `list_input_producer.py`: Produces random lists of integers to a Kafka topic.
2. `parallelism.py`: Consumes messages from the Kafka topic, calculates the sum and average of the integers in the lists using PyFlink, and prints the results.

## Prerequisites

- Python 3.7+
- Apache Kafka
- Confluent Kafka Python client
- Apache Flink
- PyFlink

## Getting Started

### Kafka Producer

The `list_input_producer.py` script connects to a Kafka broker, generates random lists of integers, and produces them to a Kafka topic named `pwdtopic`.

#### Installation

1. Install the required Python package:

   ```sh
   pip install confluent_kafka
