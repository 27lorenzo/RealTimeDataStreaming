# RealTimeDataStreaming
End-to-end data engineering pipeline to ingest and process data from an API through Apache Airflow, PostgreSQL, Apache Kafka and Cassandra. 

## Requirements

- Python 3.11
- Apache Kafka
- Apache Zookeeper
- Apache Airflow
- PysPark
- Cassandra
- Docker

## Overview

The project consists of two main scripts: `kafka-stream.py` and `spark-stream.py`.

- `kafka-stream.py`: Fetches user data from a public API, formats it, and sends it to a Kafka topic named `users_created`.
- `spark-stream.py`: Reads data from the Kafka topic, processes it using Spark, and inserts it into a Cassandra database.

All the architecture is contenirazed in Docker.

## Usage

### Streaming data into Kafka

1- Set up the environment and architecture
```
  docker-compose up -d 
  ```
2- Open Apache Airflow (http://localhost:8080/) and select Trigger DAT
3- Open Control Center (http://localhost:9021/) > Topics > users_created and watch the messages popping up

### Streaming data into Cassandra

1- Go to MVN Repository and copy the followings jar files into Lib\site-packages\pyspark\jars\:
  - spark-cassandra-connector_2.12:3.4.0
  - spark-sql-kafka-0-10_2.12:3.4.0
2- Run the below comand to start Spark and Trigger DAG in Apache Airflow
```
spark-submit --master spark://localhost:7077 --packages com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 spark-stream.py
```
3- Access the Cassandra container:
```
docker exec -it cassandra cqlsh -u cassandra -p cassandra localhost 9042
```
4- Check that the data in users_created topic is available in Cassandra:
```
SELECT * FROM spark_streams.created_users;```
