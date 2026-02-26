#!/bin/bash
set -e

# --- Configuration Section ---
# Source the config.env file if it exists, otherwise use defaults
CONFIG_FILE="/job-configs/config.env"
if [ -f "$CONFIG_FILE" ]; then
    echo "Loading configuration from $CONFIG_FILE"
    source "$CONFIG_FILE"
else
    echo "Warning: $CONFIG_FILE not found. Proceeding with environment variables..."
fi

# Health API Config (with fallbacks if not in config.env)
HEALTH_API_URL=${HEALTH_API_URL}
AUTH_TOKEN=${AUTH_TOKEN}
DEVICE_COOKIE=${DEVICE_COOKIE}
CHECK_INTERVAL_SEC=${CHECK_INTERVAL_SEC}

# Job 1: Combined Stream Processor
STREAM_JOB_JAR=${STREAM_JOB_JAR}
STREAM_JOB_CONF=${STREAM_JOB_CONF}
STREAM_JOB_CLASS=${STREAM_JOB_CLASS}
STREAM_JOB_NAME=${STREAM_JOB_NAME}

# Job 2: Combined Dashboard Creator
DASHBOARD_JOB_JAR=${DASHBOARD_JOB_JAR}
DASHBOARD_JOB_CONF=${DASHBOARD_JOB_CONF}
DASHBOARD_JOB_CLASS=${DASHBOARD_JOB_CLASS}
DASHBOARD_JOB_NAME=${DASHBOARD_JOB_NAME}
# -----------------------------

# Start JobManager in the background
echo "Starting JobManager..."
/docker-entrypoint.sh jobmanager &
JM_PID=$!

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

# Function to extract job status from JSON response
get_job_status() {
    local response=$1
    local job_name_keyword=$2
    
    # Remove all spaces and newlines from the JSON to make parsing predictable
    local compact_response=$(echo "$response" | tr -d ' \n\r')
    # Remove spaces from the search keyword so it matches the compacted JSON
    local compact_keyword=$(echo "$job_name_keyword" | tr -d ' ')
    
    local status=$(echo "$compact_response" | sed 's/{/\n{/g' | grep -i "\"name\":\"[^\"]*${compact_keyword}[^\"]*\"" | grep -o '"status":"[^"]*"' | cut -d'"' -f4 | head -n 1)
    if [ -z "$status" ]; then
        echo "NOT_FOUND"
    else
        echo "$status"
    fi
}

# Monitor and submit jobs periodically
monitor_jobs() {
    while true; do
        echo "Checking job status..."
        
        RESPONSE=$(curl -s --location "$HEALTH_API_URL" \
            --header "Authorization: ${AUTH_TOKEN}" )
    
        if [ -n "$RESPONSE" ] && [[ "$RESPONSE" != *"Connection refused"* ]] && [[ "$RESPONSE" != *"Failed to connect"* ]]; then
            STREAM_STATUS=$(get_job_status "$RESPONSE" "Stream Processor Job")
            DASHBOARD_STATUS=$(get_job_status "$RESPONSE" "Combined Dashboard Creator Job")
        else
            echo "Failed to reach health API. Assuming jobs need to be submitted..."
            # Fallback values to trigger submission
            STREAM_STATUS="NOT_FOUND"
            DASHBOARD_STATUS="NOT_FOUND"
        fi
        
        # 1. Combined Stream Processor
        if [ "$STREAM_STATUS" != "RUNNING" ]; then
            echo "Combined Stream Job status is $STREAM_STATUS. Submitting..."
            submit_job "$STREAM_JOB_JAR" "$STREAM_JOB_CONF" "$STREAM_JOB_CLASS" "$STREAM_JOB_NAME"
        else
            echo "Combined Stream Job is already RUNNING."
        fi

        # 2. Combined Dashboard Creator
        if [ "$DASHBOARD_STATUS" != "RUNNING" ]; then
            echo "Combined Dashboard Creator status is $DASHBOARD_STATUS. Submitting..."
            submit_job "$DASHBOARD_JOB_JAR" "$DASHBOARD_JOB_CONF" "$DASHBOARD_JOB_CLASS" "$DASHBOARD_JOB_NAME"
        else
            echo "Combined Dashboard Creator is already RUNNING."
        fi
        
        echo "Sleeping for $CHECK_INTERVAL_SEC seconds..."
        sleep "$CHECK_INTERVAL_SEC"
    done
}

# Run the monitor loop in the background
echo "Starting job monitor..."
monitor_jobs &
MONITOR_PID=$!

# Wait for JobManager process to exit (to keep container running)
wait $JM_PID