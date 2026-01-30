import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 1) Kafka broker (inside Docker, you mapped it to localhost:9092)
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions_topic"

# 2) Create Kafka producer (serializes dict -> JSON -> bytes)
producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

def generate_transaction():
    """Generate a fake transaction event (simulates real-time ingestion)."""
    return {
        "transaction_id": f"T{random.randint(100000, 999999)}",
        "customer_id": f"C{random.randint(1000, 9999)}",
        "amount": round(random.uniform(5, 250), 2),
        "currency": "NZD",
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "payment_method": random.choice(["card", "cash", "online"]),
        "store": random.choice(["Auckland CBD", "Glen Innes", "Manukau"]),
    }

if __name__ == "__main__":
    print(f"Producing events to Kafka topic: {TOPIC}")
    for i in range(20):
        event = generate_transaction()

        # 3) send() pushes message to Kafka topic
        #    key is optional; it helps ordering by key if you choose one
        producer.send(TOPIC, value=event)

        # 4) flush() ensures the message is actually delivered before continuing
        producer.flush()

        print(f"Sent: {event}")
        time.sleep(1)

    producer.close()
    print("Done.")
