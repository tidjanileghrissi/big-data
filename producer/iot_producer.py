from kafka import KafkaProducer
import json
import random
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    data = {
        "machine_id": "MACHINE_01",
        "temperature": round(random.uniform(60, 120), 2),
        "vibration": round(random.uniform(0.1, 5.0), 2),
        "status": random.choice(["OK", "ALERT", "ERROR"]),
        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
    }
    print("Sending:", data)
    producer.send("iot-topic", value=data)
    time.sleep(2)
