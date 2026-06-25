from kafka import KafkaProducer
import uuid
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

payment_methods = [
    "credit_card",
    "upi",
    "net_banking",
    "wallet"
]

statuses = [
    "SUCCESS",
    "FAILED"
]

while True:

    payment_event = {

        "payment_id": str(uuid.uuid4()),

        "user_id": random.randint(1000, 5000),

        "amount": round(
            random.uniform(100, 5000),
            2
        ),

        "payment_method": random.choice(
            payment_methods
        ),

        "payment_status": random.choice(
            statuses
        )
    }

    producer.send(
        "payment-events",
        value=payment_event
    )

    print(payment_event)

    time.sleep(3)