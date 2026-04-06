#!/bin/bash
JOBMANAGER_IP=$1
FLINK_HOST="http://${JOBMANAGER_IP}:8081"
JAR_NAME="users-via-csv-1.0.0.jar"
JAR_PATH="/app/metabase-jobs/users-via-csv/target/$JAR_NAME"
LOG_FILE="/app/logs/MetabaseUserUploadLogs.logs"

stop_running_jobs() {
    local jar_name=$1

    echo "Checking for running jobs associated with JAR: $jar_name..."
    JOB_IDS=$(curl -s "$FLINK_HOST/jobs/overview" | jq -r ".jobs[] | select(.name == \"$jar_name\") | .jid")

    if [ -n "$JOB_IDS" ]; then
        for JOB_ID in $JOB_IDS; do
            echo "Stopping job: $JOB_ID..."
            curl -X PATCH "$FLINK_HOST/jobs/$JOB_ID/cancel"
            echo "Job stopped: $JOB_ID."
        done
    else
        echo "No running jobs found for JAR: $jar_name."
    fi
}

delete_existing_jar() {
    local jar_name=$1

    echo "Checking for existing JAR with name: $jar_name..."
    EXISTING_JAR_ID=$(curl -s "$FLINK_HOST/jars" | jq -r ".files[] | select(.name == \"$jar_name\") | .id")

    if [ -n "$EXISTING_JAR_ID" ]; then
        echo "Deleting existing JAR: $EXISTING_JAR_ID..."
        curl -X DELETE "$FLINK_HOST/jars/$EXISTING_JAR_ID"
        echo "Existing JAR deleted: $EXISTING_JAR_ID."
    else
        echo "No existing JAR found with name: $jar_name."
    fi
}

upload_and_run_jar() {
    local jar_path=$1
    local jar_name=$(basename "$jar_path")

    delete_existing_jar "$jar_name"

    echo "Uploading JAR file to Flink: $jar_path..."
    UPLOAD_RESPONSE=$(curl -X POST -H "Expect:" -F "jarfile=@$jar_path" "$FLINK_HOST/jars/upload")

    if [ $? -ne 0 ]; then
        echo "JAR upload failed for $jar_name. Exiting..."
        exit 1
    fi

    echo "JAR uploaded successfully: $jar_name"

    JAR_ID=$(curl -s "$FLINK_HOST/jars" | jq -r ".files | sort_by(.uploaded) | last | .id")

    if [ -z "$JAR_ID" ]; then
        echo "Failed to get JAR ID for $jar_name. Exiting..."
        exit 1
    fi

    echo "JAR ID for $jar_name: $JAR_ID"

    echo "Starting Flink job for $jar_name..."
    RUN_RESPONSE=$(curl -X POST "$FLINK_HOST/jars/$JAR_ID/run")

    if [ $? -ne 0 ]; then
        echo "Failed to start Flink job for $jar_name. Exiting..."
        exit 1
    fi

    echo "Flink job started successfully for $jar_name."
}

sleep 10
upload_and_run_jar "/app/stream-jobs/project-stream-processor/target/project-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/stream-jobs/observation-stream-processor/target/observation-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/stream-jobs/survey-stream-processor/target/survey-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/stream-jobs/user-stream-processor/target/user-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/project-dashboard-creator/target/project-dashboard-creator-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/observation-dashboard-creator/target/observation-dashboard-creator-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/survey-dashboard-creator/target/survey-dashboard-creator-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/user-service/target/user-service-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/user-dashboard-creator/target/user-dashboard-creator-1.0.0.jar"
upload_and_run_jar "/app/stream-jobs/mentoring-stream-processor/target/mentoring-stream-processor-1.0.0.jar"
upload_and_run_jar "/app/metabase-jobs/mentoring-dashboard-creator/target/mentoring-dashboard-creator-1.0.0.jar"

# submitting the jar file to AKKA service
mkdir -p /app/logs
mkdir -p /app/csv
nohup java -jar /app/metabase-jobs/users-via-csv/target/users-via-csv-1.0.0.jar >> /app/logs/MetabaseUserUploadLogs.logs 2>&1 & sleep 10
ps -ef | grep users-via-csv

mkdir -p /app/logs
mkdir -p /app/csv

echo "Checking if $JAR_NAME is already running..."
PIDS=$(ps -ef | grep "$JAR_NAME" | grep -v grep | awk '{print $2}')

if [[ -n "$PIDS" ]]; then
    echo "Found running processes: $PIDS. Stopping them..."
    kill -9 $PIDS
    sleep 2  # Wait for processes to terminate
    echo "All instances of $JAR_NAME stopped."
else
    echo "No running instance of $JAR_NAME found."
fi

echo "Starting new instance of $JAR_NAME..."
nohup java -jar "$JAR_PATH" >> "$LOG_FILE" 2>&1 &

sleep 10

NEW_PIDS=$(ps -ef | grep "$JAR_NAME" | grep -v grep | awk '{print $2}')

if [[ -n "$NEW_PIDS" ]]; then
    echo "$JAR_NAME started successfully with PID(s): $NEW_PIDS"
else
    echo "Failed to start $JAR_NAME."
fi