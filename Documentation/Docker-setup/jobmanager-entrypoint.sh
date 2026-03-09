#!/bin/bash
set -e

CONFIG_FILE="/job-configs/config.env"

echo "Starting Elevate Data Flink Job Runner..."

# ---------------------------------------------------
# Load configuration
# ---------------------------------------------------
if [ -f "$CONFIG_FILE" ]; then
    echo "Loading configuration from $CONFIG_FILE"
    source "$CONFIG_FILE"
else
    echo "ERROR: config.env not found!"
    exit 1
fi

HEALTH_API_URL=${HEALTH_API_URL}
AUTH_TOKEN=${AUTH_TOKEN}
CHECK_INTERVAL_SEC=${CHECK_INTERVAL_SEC:-60}

FLINK_URL=${FLINK_URL}
JOB_JAR_JSON=${JOB_JAR}
JOB_CONF_ARRAY=${JOB_CONF}

# ---------------------------------------------------
# Helpers
# ---------------------------------------------------

parse_array() {
    echo "$1" | sed 's/\[//g;s/\]//g;s/"//g' | tr ',' '\n'
}

get_entry_class_from_jar() {
    # Read Main-Class directly from the JAR's MANIFEST.MF.
    # We must unfold continuation lines (lines starting with a space) because 
    # Java manifests wrap lines at 72 bytes.
    JAR=$1
    unzip -p "$JAR" META-INF/MANIFEST.MF \
        | tr -d '\r' \
        | awk '/^[ \t]/ { printf "%s", substr($0, 2); next } { printf "%s%s", (NR==1 ? "" : "\n"), $0 } END { printf "\n" }' \
        | grep "^Main-Class:" \
        | awk '{print $2}'
}

get_config_b64() {
    JOB_CONF=$1
    cat "$JOB_CONF" | base64 -w 0
}

wait_for_flink() {
    echo "Waiting for Flink JobManager..."

    for i in {1..30}; do
        if curl -s "$FLINK_URL/overview" >/dev/null; then
            echo "Flink JobManager is ready."
            return
        fi

        echo "Waiting..."
        sleep 2
    done

    echo "Flink JobManager did not start."
    exit 1
}

upload_jar() {
    JAR=$1

    RESPONSE=$(curl -s -X POST -H "Expect:" \
        -F "jarfile=@$JAR" \
        "$FLINK_URL/jars/upload")

    echo "$RESPONSE" | grep -o '"filename":"[^"]*"' | \
    awk -F'"' '{print $4}' | awk -F'/' '{print $NF}'
}

submit_job() {

    JAR=$1
    CONF=$2

    CONFIG_B64=$(get_config_b64 "$CONF")

    echo "----------------------------------------"
    echo "Submitting Job"
    echo "Jar   : $JAR"
    echo "Conf  : $CONF (base64 inline)"
    echo "----------------------------------------"

    # Extract entry class from JAR manifest before uploading
    CLASS=$(get_entry_class_from_jar "$JAR")

    echo "Detected Entry Class: $CLASS"

    if [ -z "$CLASS" ]; then
        echo "ERROR: Could not detect entry class from JAR manifest. Aborting job submission."
        return 1
    fi

    # Upload JAR
    JAR_ID=$(upload_jar "$JAR")

    if [ -z "$JAR_ID" ]; then
        echo "Jar upload failed"
        return
    fi

    echo "Uploaded jar id: $JAR_ID"

    # Run Job
    curl -s -X POST \
        "$FLINK_URL/jars/$JAR_ID/run" \
        -H "Content-Type: application/json" \
        -d "{
            \"entryClass\": \"$CLASS\",
            \"programArgs\": \"--config.content $CONFIG_B64\"
        }"

    echo "Job submitted."
}

check_api_job_status() {
    local JOB_NAME=$1
    local API_URL=${HEALTH_API_URL}
    local TOKEN=${AUTH_TOKEN}

    local RESPONSE=$(curl -s --location "$API_URL" --header "Authorization: $TOKEN")

    echo "$RESPONSE" | grep -A 1 "\"name\": \"$JOB_NAME\"" | grep "\"status\": \"RUNNING\"" >/dev/null
}

# ---------------------------------------------------
# Job monitor loop
# ---------------------------------------------------

monitor_jobs() {

    # Extract single conf path from array
    CONF=$(parse_array "$JOB_CONF_ARRAY" | head -n 1)

    while true
    do
        echo "Checking Flink jobs..."

        # Parse JSON and process each job
        python3 -c "import sys, json; data=json.loads(sys.argv[1]); [print(f'{k}|{v}') for k,v in data.items()]" "$JOB_JAR_JSON" | while IFS='|' read -r name jar
        do
            if check_api_job_status "$name"; then
                echo "Job '$name' is already running."
            else
                echo "Submitting job '$name'..."
                submit_job "$jar" "$CONF"
            fi
        done

        echo "Sleeping $CHECK_INTERVAL_SEC seconds..."
        sleep "$CHECK_INTERVAL_SEC"

    done
}

# ---------------------------------------------------
# Start
# ---------------------------------------------------

wait_for_flink
monitor_jobs