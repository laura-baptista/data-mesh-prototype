from kafka import KafkaProducer
import json
import time
import uuid
import random


BOOTSTRAP_SERVERS = "localhost:9093"

TOPICS = {
    "create": "clients.create.v1",
    "update": "clients.update.v1",
    "delete": "clients.delete.v1",
}


def create_event():
    return {
        "event_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "name": random.choice(["Ana", "Bruno", "Carlos", "Daniela", "Eduardo", "Fernanda"]),
        "email": f"user{random.randint(1,500)}@example.com",
        "timestamp": time.time(),
    }


def update_event():
    return {
        "event_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "changed_field": "email",
        "new_value": f"user{random.randint(1,500)}@newmail.com",
        "timestamp": time.time(),
    }


def delete_event():
    return {
        "event_id": str(uuid.uuid4()),
        "client_id": random.randint(1000, 9999),
        "reason": "User request",
        "timestamp": time.time(),
    }


EVENT_GENERATORS = {
    "create": create_event,
    "update": update_event,
    "delete": delete_event,
}

def main():
    # Configuração correta para confluent-kafka
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

    print("\nSending CLIENT events...\n")

    for event_type, topic in TOPICS.items():
        event = EVENT_GENERATORS[event_type]()
        payload = json.dumps(event).encode("utf-8")

        producer.send(
            topic=topic,
            value=payload
        )

        print(f"INFO: Sending to {topic}: {event}")

    # Aguarda envio completo
    producer.flush()
    print("\nINFO: Done\n")


if __name__ == "__main__":
    main()
