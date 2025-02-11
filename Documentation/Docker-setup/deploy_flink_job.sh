#!/bin/bash

# Load environment variables from .env file
set -a
source ./config.env
set +a

# Set environment variables
KAFKA_CONTAINER="${KAFKA_CONTAINER_NAME}"
ELEVATE_CONTAINER="${ELEVATE_DATA_CONTAINER_NAME}"

# Read Kafka topic names from environment variables or use defaults
TOPIC1="${PROJECT_TOPIC}"
TOPIC2="${METABASE_TOPIC}"

# Load report_config data in postgres
echo "Loading report_config data in Postgres..."
 docker exec -it elevate-data chmod +x /app/metabase-jobs/config-data-loader/data-loader.sh
 docker exec -it elevate-data /app/metabase-jobs/config-data-loader/data-loader.sh $PG_DBNAME $POSTGRES_USER $POSTGRES_PASSWORD $POSTGRES_HOST $POSTGRES_PORT $PG_ENV
echo "report_config data loaded successfully."

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

# 3. Download the JAR file from the Elevate Data container
echo "Copying JAR files from $ELEVATE_CONTAINER to local machine..."
sudo docker cp elevate-data:/app/project-jobs/project-stream-processor/target/project-stream-processor-1.0.0.jar ./
sudo docker cp elevate-data:/app/metabase-jobs/dashboard-creator/target/dashboard-creator-1.0.0.jar ./
echo "JAR files copied successfully."

# 4. Upload and run JARs in Flink
upload_and_run_jar() {
    local jar_file=$1

    echo "Uploading JAR file to Flink: $jar_file..."
    UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@./$jar_file" "http://localhost:8081/jars/upload")

    if [ $? -ne 0 ]; then
        echo "JAR upload failed. Exiting..."
        exit 1
    fi

    echo "JAR uploaded successfully."

    # Extract the uploaded JAR ID
    JAR_ID=$(curl -s http://localhost:8081/jars | jq -r '.files[0].id')

    if [ -z "$JAR_ID" ]; then
        echo "Failed to get JAR ID. Exiting..."
        exit 1
    fi

    echo "JAR ID: $JAR_ID"

    # Run the JAR file in Flink
    echo "Starting Flink job for $jar_file..."
    RUN_RESPONSE=$(curl -X POST "http://localhost:8081/jars/$JAR_ID/run")

    if [ $? -ne 0 ]; then
        echo "Failed to start Flink job. Exiting..."
        exit 1
    fi

    echo "Flink job started successfully for $jar_file."
}

upload_and_run_jar "project-stream-processor-1.0.0.jar"
upload_and_run_jar "dashboard-creator-1.0.0.jar"
