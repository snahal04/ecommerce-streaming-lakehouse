# Ecommerce Lakehouse with Kafka and Spark

This project is a simple real-time data engineering pipeline that demonstrates how to ingest event data from Apache Kafka, process it with Apache Spark, and store it in a lakehouse-style structure using Parquet files.

It is designed as a hands-on learning project for:
- Kafka fundamentals
- Spark Structured Streaming
- Real-time data pipelines
- Bronze/Silver data layer concepts
- Local development with Docker

---

## 1. Project Overview

The pipeline simulates ecommerce events and streams them through Kafka into Spark streaming jobs.

### End-to-end flow
1. A Python producer generates ecommerce events.
2. Events are published to a Kafka topic.
3. A Spark consumer reads from Kafka.
4. Data is written to the bronze layer as Parquet files.
5. A second Spark job processes and cleans the data for the silver layer.
6. Invalid records can be quarantined for later review.

### What this project teaches
- How Kafka topics, partitions, and offsets work
- How Spark Structured Streaming consumes from Kafka
- How to build a mini lakehouse architecture locally
- How checkpointing helps streaming jobs recover safely
- How to think about real-time pipeline design for interviews

---

## 2. Architecture Summary

### Components
- Kafka: message broker for streaming events
- Producer: Python script that sends JSON events
- Spark Consumer: reads Kafka topics and writes Parquet files
- Bronze Layer: raw ingested events
- Silver Layer: cleaned and parsed events
- Quarantine Layer: invalid or malformed records
- Checkpoints: state tracking for streaming jobs

### Basic architecture diagram

```text
Producer (Python) --> Kafka Topic --> Spark Structured Streaming --> Bronze Parquet
                                                    --> Silver Parquet
                                                    --> Quarantine Parquet
```

---

## 3. Repository Structure

```text
├── consumer/                  # Spark streaming consumers
│   ├── bronze_stream.py
│   ├── payment_bronze_stream.py
│   ├── silver_stream.py
│   └── gold/                  # gold layer scripts
├── producer/                  # Kafka event producers
│   ├── event_generator.py
│   └── payment_generator.py
├── common/                    # shared helper logic
├── data/                      # sample data and import scripts
├── checkpoints/               # Spark checkpoint directories
├── bronze/                    # raw streamed output
├── silver/                    # cleaned output
├── quarantine/                # invalid records
├── docker-compose.yml         # Kafka container setup
├── requirements.txt           # Python dependencies
└── README.md                  # project documentation
```

---

## 4. Tech Stack

- Python 3
- Apache Kafka
- Apache Spark 3.5.6
- PySpark
- Docker Compose
- Parquet
- Kafka Python Client
- Faker (for synthetic event generation)

---

## 5. Prerequisites

Before running this project, install the following on your local machine:

- Docker Desktop or Docker Engine
- Docker Compose
- Python 3.9+
- Java (required for Spark)
- Apache Spark (or use PySpark from Python)

### Check versions

```bash
python3 --version
java -version
docker --version
docker compose version
```

---

## 6. Local Setup

### Step 1: Clone the repository

```bash
git clone <your-repo-url>
cd ecommerce-lakehouse
```

### Step 2: Create a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Start Kafka using Docker Compose

```bash
docker compose up -d
```

### Step 5: Verify Kafka container is running

```bash
docker ps
```

You should see the Kafka container running.

---

## 7. Running the Project

### A. Start the Kafka broker

If you haven’t already started it:

```bash
docker compose up -d
```

### B. Create Kafka topics (if needed)

The project uses Kafka topics such as:
- ecommerce-events
- payment-events

Create a topic like this:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic ecommerce-events --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Another example:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic user-login-events \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1
```

### C. List active topics

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### D. Describe topics and partitions

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
```

Example output will show partition and replication details:

```text
Topic: ecommerce-events TopicId: ... PartitionCount: 4 ReplicationFactor: 1 Configs: min.insync.replicas=1
```

---

## 8. Run the Producer

Start the event generator so Kafka receives synthetic ecommerce events:

```bash
python producer/event_generator.py
```

This script sends JSON events with fields like:
- event_id
- user_id
- session_id
- event_time
- event_type
- product_id
- amount

---

## 9. Run the Spark Consumers

### Bronze stream

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 consumer/bronze_stream.py
```

This job reads from Kafka and writes raw messages to the bronze folder.

### Payment bronze stream

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 consumer/payment_bronze_stream.py
```

### Silver stream

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 consumer/silver_stream.py
```

This job parses JSON events, applies schema handling, and writes cleaned records to the silver layer.

---

## 10. What Happens in Each Layer

### Bronze Layer
- Stores raw ingested events from Kafka
- Preserves source data as-is as much as possible
- Good for replay and reprocessing

### Silver Layer
- Applies parsing and transformations
- Cleans malformed or invalid records
- Adds useful derived fields like event date

### Quarantine Layer
- Stores records that fail quality checks
- Useful for debugging bad data

### Gold Layer
- To Create KPI and later can be used in Power BI
---

## 11. Kafka Fundamentals for Interviews

Here is a practical interview-oriented explanation of Kafka concepts.

### 1. Kafka Architecture
Kafka is a distributed event streaming platform.

Main components:
- Producer: sends data to Kafka
- Broker: stores and serves messages
- Consumer: reads messages from topics
- Topic: logical stream of events
- Partition: parallel unit of storage and consumption
- Cluster: group of brokers working together

### 2. Producer
A producer publishes records to a Kafka topic.

Important points:
- Producers choose the partition for a message
- They can send synchronously or asynchronously
- Acks determine durability guarantees

### 3. Consumer
A consumer reads messages from Kafka topics.

Important points:
- Consumers read from offsets
- Consumers can be part of a consumer group
- They process data independently but coordinate with the group

### 4. Consumer Groups
A consumer group lets multiple consumers share the load of a topic.

Example:
- One topic with 3 partitions
- 3 consumers in a group can each read one partition

Important idea:
- Each partition is consumed by only one consumer in a group at a time

### 5. Offsets
Offsets are the position of a consumer inside a partition.

Why they matter:
- They track progress
- They allow replay and recovery
- They support exactly-once-ish processing when managed correctly

### 6. Partitions
Partitions split a topic into multiple logs.

Why they matter:
- Increase parallelism
- Improve throughput
- Allow multiple consumers to read concurrently

### 7. Replication
Replicas keep copies of partitions across brokers.

Why it matters:
- Fault tolerance
- High availability
- Protection from broker failure

### 8. Delivery Semantics
Kafka supports delivery guarantees:
- At most once: messages may be lost
- At least once: messages may be duplicated
- Exactly once: messages are processed once and only once, with careful design

### 9. Kafka Connect
Kafka Connect is used to integrate Kafka with external systems such as:
- databases
- file systems
- cloud storage
- data lakes

It is commonly used for source and sink connectors.

### 10. Schema Registry
Schema Registry stores and manages schemas for events.

Why it is useful:
- Schema evolution
- Compatibility checks
- Consistency across producers and consumers

### 11. Kafka Streams Basics
Kafka Streams is a library for stream processing inside Kafka.

Typical use cases:
- filtering
- transformation
- aggregation
- joins
- windowing

### 12. Spark + Kafka Integration
Spark Structured Streaming can read from Kafka topics and write results to storage systems.

This project uses that pattern with:
- Kafka as the source
- Spark as the processing engine
- Parquet as the storage format

### 13. Consumer Lag
Consumer lag means a consumer is falling behind the latest messages in a topic.

It usually happens when:
- processing is slow
- partitions are overloaded
- consumers crash or stop

### 14. Fault Tolerance
Kafka and Spark both support recovery through offsets and checkpoints.

In this project:
- Spark checkpointing helps resume streaming jobs
- Kafka offsets help consumers continue from the last processed position

### 15. Real-Time Pipeline Design
A good real-time pipeline usually includes:
- ingestion
- transformation
- validation
- storage
- monitoring
- replay support

Important design questions:
- What happens if a consumer fails?
- How do you handle late data?
- How do you ensure data quality?
- How do you scale for more traffic?

---

### Advanced Kafka Topic
1. EOS (Exactly Once Semantics)
2. Transactions
3. ISR internals
4. Rebalancing protocols
5. KRaft
6. Stream processing internals
7. Performance tuning
8. Security
9. Cluster sizing
10. Capacity planning

### Quick interview tip
If asked about Kafka in an interview, explain it as:

> Kafka is a distributed, fault-tolerant event streaming platform used to ingest, store, and process real-time data at scale.

---

## 12. Common Troubleshooting

### Kafka container not starting
```bash
docker compose down
docker compose up -d
```

### Topic not found
```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Spark submit fails
Check:
- Java is installed
- Python environment is active
- Kafka broker is reachable at localhost:9092
- The required package is available

### No data in bronze or silver output
Check:
- Producer is running
- Topic exists
- Consumer is running with the correct topic name
- Checkpoints are not blocking replay unexpectedly

---

## 13. How to Explain This Project in an Interview

You can describe it like this:

> I built a real-time data pipeline where events are generated by a Python producer, published to Kafka, consumed by Spark Structured Streaming, and stored in bronze and silver layers. The project demonstrates event streaming, checkpointing, partition-based parallelism, and lakehouse-style data processing.

---

## 15. Next Steps

You can extend this project by adding:
- Schema Registry for schema validation
- Kafka Connect for database/file integration
- Kafka Streams for transformations
- Monitoring and alerting
- Dockerized Spark service

---

## 16. Summary

This repository is a practical starting point for learning real-time data engineering with Kafka and Spark. It shows how to move from raw event ingestion to processed data in a simple but realistic architecture.

If you understand the flow in this project, you will be well prepared for most beginner-to-intermediate Kafka and Spark interview questions.