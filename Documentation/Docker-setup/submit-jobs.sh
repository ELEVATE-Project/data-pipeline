#!/bin/bash
JOBMANAGER_IP=$1
FLINK_HOST="http://${JOBMANAGER_IP}:8081"  # Change if necessary

upload_and_run_jar() {
    local jar_path=$1
    local jar_name=$(basename "$jar_path")

    echo "Uploading JAR file to Flink: $jar_path..."
    UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@$jar_path" "$FLINK_HOST/jars/upload")

    if [ $? -ne 0 ]; then
        echo "JAR upload failed for $jar_name. Exiting..."
        exit 1
    fi

    echo "JAR uploaded successfully: $jar_name"

    # Extract the correct JAR ID using its name
    JAR_ID=$(curl -s "$FLINK_HOST/jars" | jq -r '.files | sort_by(.uploaded) | last | .id')

    if [ -z "$JAR_ID" ]; then
        echo "Failed to get JAR ID for $jar_name. Exiting..."
        exit 1
    fi

    echo "JAR ID for $jar_name: $JAR_ID"

    # Run the JAR file in Flink
    echo "Starting Flink job for $jar_name..."
    RUN_RESPONSE=$(curl -X POST "$FLINK_HOST/jars/$JAR_ID/run")

    if [ $? -ne 0 ]; then
        echo "Failed to start Flink job for $jar_name. Exiting..."
        exit 1
    fi

    echo "Flink job started successfully for $jar_name."
}

upload_and_run_jar "/app/project-jobs/project-stream-processor/target/project-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/dashboard-creator/target/dashboard-creator-1.0.0.jar"
