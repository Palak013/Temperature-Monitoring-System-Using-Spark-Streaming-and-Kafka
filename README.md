# Temperature-Monitoring-System-Using-Spark-Streaming-and-Kafka
This project implements a real-time temperature monitoring system using Apache Kafka and Apache Spark Streaming. The system processes temperature sensor data and classifies high temperatures (above 80°C) into alert levels. It integrates Kafka for real-time data streaming and Spark Streaming for data processing and filtering.

Features:
Kafka Producer: Simulates temperature sensor data and sends it to a Kafka topic (temperature-sensors).

Kafka Consumer: Consumes data from the Kafka topic and processes it using Spark Streaming.

Spark Streaming: Filters temperatures greater than 80°C and classifies them into alert levels.

Real-Time Processing: The system can handle both simulated data and real-time data streams, providing an efficient way to monitor temperature fluctuations.

Technology Stack:
Apache Kafka: Used for real-time message streaming and data transport.

Apache Spark Streaming: For processing the incoming temperature data in real-time.

Scala: Programming language for writing the Spark Streaming application.

JSON: Format for simulating temperature sensor data.

Setup Instructions:
Install Apache Kafka and Apache Spark.

Start Zookeeper and Kafka Server.

Run Kafka Producer to simulate temperature sensor data.

Run Kafka Consumer to consume and process the data using Spark Streaming.

Monitor the output in the console, where alerts for high-temperature readings will be displayed.

Commands and Setup:
The setup includes detailed instructions for starting Kafka and Spark servers, producing and consuming messages, and running the Spark Streaming application.

