import uuid
import json
import time
from confluent_kafka import Producer

producer_config = {"bootstrap.servers": "kafka:9092"}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()}: partition {msg.partition()} at offset {msg.offset()}")

print("Producer started. Sending every 3 seconds...")
while True:
    order = {
        "order_id": str(uuid.uuid4()),
        "user": "nana",
        "item": "mushroom pizza",
        "quantity": 6
    }
    producer.produce(
        topic="orders",
        key=order["order_id"].encode("utf-8"),
        value=json.dumps(order).encode("utf-8"),
        callback=delivery_report
    )
    producer.flush()
    time.sleep(3)
