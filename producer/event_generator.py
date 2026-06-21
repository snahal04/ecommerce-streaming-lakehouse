from kafka import KafkaProducer
from faker import Faker
import uuid
import json
import random
import time
from datetime import datetime

fake = Faker()

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

event_types = ["view", "add_to_cart", "purchase"]

previous_event = None

while True:

    # event = {
    #     "event_id": str(uuid.uuid4()),
    #     "user_id": random.randint(1, 1000),
    #     "session_id": random.randint(10000, 99999),
    #     "event_time": datetime.utcnow().isoformat(),
    #     "event_type": random.choice(event_types),
    #     "product_id": f"P{random.randint(1,100)}",
    #     "amount": round(random.uniform(100, 5000), 2)
    # }
    is_duplicate = False
    if previous_event and random.random() < 0.4:
        event = previous_event
        is_duplicate = True
    else:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1, 1000),
            "session_id": random.randint(10000, 99999),
            "event_time": datetime.utcnow().isoformat(),
            "event_type": random.choice(event_types),
            "product_id": f"P{random.randint(1,100)}",
            "amount": round(random.uniform(100, 5000), 2),
        }

    previous_event = event
    # Send the event to Kafka
    future = producer.send("ecommerce-events", value=event)
    record_metadata = future.get(timeout=10)

    # Clean, simple console log output
    dup_label = "[DUPLICATE] " if is_duplicate else ""
    print(
        f"{dup_label}Event ID: {event['event_id']} | "
        f"Partition: {record_metadata.partition} | "
        f"Offset: {record_metadata.offset}"
    )

    time.sleep(6)
