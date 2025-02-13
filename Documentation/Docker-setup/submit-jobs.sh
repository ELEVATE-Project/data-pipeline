#!/bin/bash
JOBMANAGER_IP=$1
FLINK_HOST="http://${JOBMANAGER_IP}:8081"  # Change if necessary

upload_and_run_jar() {
    local jar_path=$1

    echo "Uploading JAR file to Flink: $jar_path..."
    UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@$jar_path" "$FLINK_HOST/jars/upload")

    if [ $? -ne 0 ]; then
        echo "JAR upload failed. Exiting..."
        exit 1
    fi

    echo "JAR uploaded successfully."

    # Extract the uploaded JAR ID
    JAR_ID=$(curl -s "$FLINK_HOST/jars" | jq -r '.files[-1].id')

    if [ -z "$JAR_ID" ]; then
        echo "Failed to get JAR ID. Exiting..."
        exit 1
    fi

    echo "JAR ID: $JAR_ID"

    # Run the JAR file in Flink
    echo "Starting Flink job for $jar_path..."
    RUN_RESPONSE=$(curl -X POST "$FLINK_HOST/jars/$JAR_ID/run")

    if [ $? -ne 0 ]; then
        echo "Failed to start Flink job. Exiting..."
        exit 1
    fi

    echo "Flink job started successfully for $jar_path."
}

upload_and_run_jar "/app/project-jobs/project-stream-processor/target/project-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/dashboard-creator/target/dashboard-creator-1.0.0.jar"
