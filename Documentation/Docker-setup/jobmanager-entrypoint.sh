#!/bin/bash
set -e

# Start JobManager in the background
echo "Starting JobManager..."
/docker-entrypoint.sh jobmanager &
JM_PID=$!

# Wait for JobManager to be ready
echo "Waiting for JobManager to be ready..."
# Wait for JobManager to be ready
echo "Waiting for JobManager to be ready..."
for i in {1..30}; do
    if (echo > /dev/tcp/localhost/8081) >/dev/null 2>&1; then
        echo "JobManager is up!"
        break
    fi
    echo "Waiting..."
    sleep 2
done

# Function to submit a job
submit_job() {
    JAR_PATH=$1
    CONF_PATH=$2
    CLASS_NAME=$3
    JOB_NAME=$4

    echo "--------------------------------------------------"
    echo "Submitting Job: $JOB_NAME"
    echo "JAR: $JAR_PATH"
    echo "Config: $CONF_PATH"
    echo "--------------------------------------------------"

    flink run -d \
        -c "$CLASS_NAME" \
        "$JAR_PATH" \
        --config.file.path "$CONF_PATH"
    
    echo "Job '$JOB_NAME' submitted."
}

# Define Job Configurations
# 1. Combined Stream Processor
submit_job \
    "/job-jars/combined-stream-processor-1.0.0.jar" \
    "/job-configs/combined-stream.conf" \
    "org.shikshalokam.job.combined.stream.processor.task.UnifiedStreamTask" \
    "combined-stream-processor"

# 2. Combined Dashboard Creator
submit_job \
    "/job-jars/combined-dashboard-creator-1.0.0.jar" \
    "/job-configs/combined-dashboard-creator.conf" \
    "org.shikshalokam.job.combined.dashboard.creator.task.CombinedDashboardCreatorTask" \
    "combined-dashboard-creator"


echo "All jobs submitted."

# Wait for JobManager process to exit (to keep container running)
wait $JM_PID
