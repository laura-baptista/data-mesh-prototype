from kafka import KafkaProducer
import json
import time
import uuid
import random

BOOTSTRAP_SERVERS = "localhost:9093"

TOPICS = {
    "create": "sales.create.v1",
    "update": "sales.update.v1",
    "delete": "sales.delete.v1",
}

def create_event():
    return {
        "event_id": str(uuid.uuid4()),
        "sale_id": random.randint(10000, 99999),
        "client_id": random.randint(1000, 9999),
        "price": round(random.uniform(50, 5000), 2),
        "items": random.randint(1, 10),
        "timestamp": time.time(),
    }

def update_event():
    return {
        "event_id": str(uuid.uuid4()),
        "sale_id": random.randint(10000, 99999),
        "changed_field": "price",
        "new_value": round(random.uniform(50, 5000), 2),
        "timestamp": time.time(),
    }

def delete_event():
    return {
        "event_id": str(uuid.uuid4()),
        "sale_id": random.randint(10000, 99999),
        "reason": "Cancelation",
        "timestamp": time.time(),
    }

EVENT_GENERATORS = {
    "create": create_event,
    "update": update_event,
    "delete": delete_event,
}

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print("Sending SALES events")

    for event_type, topic in TOPICS.items():
        event = EVENT_GENERATORS[event_type]()
        producer.send(topic, event)
        print(f"Sent to {topic}: {event}")

    producer.flush()
    print("Done")

if __name__ == "__main__":
    main()
