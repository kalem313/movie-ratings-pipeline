# Real-Time Movie Ratings Pipeline with Kafka, Spark & Cassandra

This project demonstrates a full end-to-end big data streaming pipeline built using:

- **Apache Kafka** – for ingesting user-generated movie ratings
- **Apache Spark Structured Streaming** – for enriching the ratings with Netflix metadata
- **Apache Cassandra** – for persistent, queryable storage
- **Vagrant + Docker Compose** – for reproducible deployment of the data infrastructure

> Developed as part of the *Large Scale Data Management* course @ AUEB (Athens University of Economics and Business)  
> Author: **Kalemkeridis Evangelos Rafail**

---

## Project Structure

```bash
movie-ratings-pipeline/
├── kafka_producer.py                  # Generates fake user ratings to Kafka (Faker)
├── cassandra_spark_streaming.py      # Spark consumer: joins ratings with metadata, writes to Cassandra
├── data/
│   ├── movies.csv                     # Movie names for random selection
│   └── netflix.csv                    # Netflix movie metadata (director, year, duration, etc.)
├── ratings_sample.csv                # Sample of 50 persisted ratings (for inspection/demo)
├── docker-compose-kafka.yml          # Kafka + Zookeeper setup
├── docker-compose-cassandra.yml      # Cassandra DB setup
├── docker-compose-spark.yml          # Spark Master/Worker nodes
├── Vagrantfile                        # VM setup for consistent environment
├── README.md                          # You're reading it
