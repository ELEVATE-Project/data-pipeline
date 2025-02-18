#!/bin/bash

# Load environment variables from .env file
set -a
source ./config.env
set +a

# Set environment variables
KAFKA_CONTAINER="${KAFKA_CONTAINER_NAME}"
ELEVATE_CONTAINER="${ELEVATE_DATA_CONTAINER_NAME}"
FLINK_JOBMANAGER_CONTAINER="${FLINK_JOBMANAGER_CONTAINER_NAME}"

# Read Kafka topic names from environment variables or use defaults
TOPIC1="${PROJECT_TOPIC}"
TOPIC2="${METABASE_TOPIC}"

# 1. Create Kafka topics in the Docker container
echo "Creating Kafka topics..."
 sudo docker exec -it $KAFKA_CONTAINER /usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic $TOPIC1

 sudo docker exec -it $KAFKA_CONTAINER /usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic $TOPIC2

echo "Kafka topics created successfully."

# 2. Build the Maven JAR inside the Elevate Data container
echo "Building Maven project inside $ELEVATE_CONTAINER..."
sudo docker exec -it $ELEVATE_CONTAINER mvn clean install -DskipTests

if [ $? -ne 0 ]; then
    echo "Maven build failed. Exiting..."
    exit 1
fi

echo "Maven build completed successfully."

# 5. submit the flink job
 sudo docker exec -it elevate-data /app/Documentation/Docker-setup/submit-jobs.sh $FLINK_JOBMANAGER_CONTAINER