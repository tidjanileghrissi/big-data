# IoT Monitoring System using Kafka

This project simulates IoT sensors sending machine data to a Kafka topic.

## Structure

- `producer/`: Simulates sending sensor data.
- `consumer/`: Receives and processes sensor data.
- Kafka is expected to be running on `localhost:9092`.

## How to Run

### 1. Start Zookeeper and Kafka
Using CMD or PowerShell in Kafka folder:

```bash
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
