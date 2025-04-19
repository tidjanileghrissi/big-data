from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'iot-topic',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: m.decode('utf-8'),  # لا تحاول تحميل JSON الآن
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

print("Listening for messages...")

for message in consumer:
    raw_value = message.value
    try:
        data = json.loads(raw_value)
        print(f"[✔] JSON Received: {data}")
    except json.JSONDecodeError:
        print(f"[⚠] Non-JSON or Invalid message skipped: {raw_value}")
