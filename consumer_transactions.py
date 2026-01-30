import json
from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "transactions_topic"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",   # read from beginning (good for assessment evidence)
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="transactions-consumer-group"
)

print(f"Listening to topic: {TOPIC}")
for msg in consumer:
    print("Received:", msg.value)
