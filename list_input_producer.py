from confluent_kafka import Producer
import json
import time
import random

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'kafka-python-producer'
}

# Create Producer instance
producer = Producer(conf)

# Kafka topic
topic = "pwdtopic"

def delivery_report(err, msg):
    if err is not None:
        print(f'Delivery failed for Message {msg.key()}: {err}')
    else:
        print(f'Message {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def generate_random_list(length=5, min_value=1, max_value=10):
    return [random.randint(min_value, max_value) for _ in range(length)]

try:
    while True:
        random_list = generate_random_list()
        message = json.dumps(random_list)
        producer.produce(topic, key=None, value=message, callback=delivery_report)
        producer.poll(1)
        print(f"Produced message: {message}")

        # Wait for 1 second before producing the next message
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped producing messages.")

# Wait for any outstanding messages to be delivered and delivery report callbacks to be triggered
producer.flush()
