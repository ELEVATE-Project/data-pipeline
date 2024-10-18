#!/bin/bash

## This bash script automates the process of creating a new collection and dashboard in Metabase,
## retrieves relevant IDs (collection, dashboard, and database), and saves them to a file.
## It uses curl for API requests and jq to handle JSON responses.

# External Path 
report_path=$1
main_dir_path=$2
report_name=$(echo "$3" | tr -s '-' ' ')
parameter_value1=$4
parameter_value2=$5

# ANSI escape codes for colors
BOLD_YELLOW="\033[1;33m"
NC="\033[0m" 

echo -e "${BOLD_YELLOW}         :: Starting up the $report_name ::          ${NC}"
echo -e "${NC}"

# Metabase server URL and credentials
METABASE_URL="http://localhost:3000"
METABASE_USERNAME="prashanth@shikshalokam.org"
METABASE_PASSWORD="Pg@9890360246"

# Collection and dashboard details
if [[ "$report_name" == "District Report" ]]; then
    COLLECTION_NAME=$(echo "$report_name" | sed 's/ Report$//; s/$/ Collection ['"$parameter_value2"']/')
    DASHBOARD_NAME="$report_name [$parameter_value2]"
elif [[ "$report_name" == "Super Admin Report" ]]; then
    COLLECTION_NAME=$(echo "$report_name" | sed 's/ Report$//; s/$/ Collection /')
    DASHBOARD_NAME="$report_name"
else
    COLLECTION_NAME=$(echo "$report_name" | sed 's/ Report$//; s/$/ Collection ['"$parameter_value1"']/')
    DASHBOARD_NAME="$report_name [$parameter_value1]"
fi

echo "COLLECTION_NAME: $COLLECTION_NAME"
echo "DASHBOARD_NAME: $DASHBOARD_NAME"

OUTPUT_FILE="$main_dir_path/metadata_file.txt"
DATABASE_NAME="meta-data" # Local DB
METADATA_LOG_FILE="$main_dir_path/$3-Metadata.txt"

# Check if required commands are installed
if ! command -v curl &> /dev/null; then
    echo "Error: curl is not installed. Please install curl to use this script."
    exit 1
fi

if ! command -v jq &> /dev/null; then
    echo "Error: jq is not installed. Please install jq to use this script."
    exit 1
fi

# Step 1: Get the session token
session_response=$(curl --silent --location --request POST "$METABASE_URL/api/session" \
--header "Content-Type: application/json" \
--data-raw "{
  \"username\": \"$METABASE_USERNAME\",
  \"password\": \"$METABASE_PASSWORD\"
}")

# Extract the session token using jq
SESSION_TOKEN=$(echo "$session_response" | jq -r '.id')

# Check if session token was created successfully
if [ -z "$SESSION_TOKEN" ]; then
    echo "Error: Failed to obtain Metabase session token. Response: $session_response"
    exit 1
else
    echo ">>  Session Token obtained successfully."
fi

# Step 2: Create a new collection
collection_response=$(curl --silent --location --request POST "$METABASE_URL/api/collection" \
--header "Content-Type: application/json" \
--header "X-Metabase-Session: $SESSION_TOKEN" \
--data-raw "{
  \"name\": \"$COLLECTION_NAME\",
  \"description\": \"Collection for Improvement Projects\"
}")

# Extract the collection ID using jq
COLLECTION_ID=$(echo "$collection_response" | jq -r '.id')

# Check if the collection was created successfully
if [ -n "$COLLECTION_ID" ]; then
    echo ">>  Collection created successfully with ID: $COLLECTION_ID"
else
    echo "Error: Failed to create the collection. Response: $collection_response"
    exit 1
fi

# Step 3: Create a new dashboard in the collection
dashboard_response=$(curl --silent --location --request POST "$METABASE_URL/api/dashboard" \
--header "Content-Type: application/json" \
--header "X-Metabase-Session: $SESSION_TOKEN" \
--data-raw "{
  \"name\": \"$DASHBOARD_NAME\",
  \"collection_id\": $COLLECTION_ID
}")

# Extract the dashboard ID using jq
DASHBOARD_ID=$(echo "$dashboard_response" | jq -r '.id')

# Check if the dashboard was created successfully
if [ -n "$DASHBOARD_ID" ]; then
    echo ">>  Dashboard created successfully with ID: $DASHBOARD_ID"
else
    echo "Error: Failed to create the dashboard. Response: $dashboard_response"
    exit 1
fi

# Step 4: Extract database ID based on database name
databases_response=$(curl --silent --location --request GET "$METABASE_URL/api/database" \
--header "Content-Type: application/json" \
--header "X-Metabase-Session: $SESSION_TOKEN")
DATABASE_ID=$(echo "$databases_response" | jq -r --arg DATABASE_NAME "$DATABASE_NAME" '.data[] | select(.name == $DATABASE_NAME) | .id')

# Check if database ID was found
if [ -n "$DATABASE_ID" ]; then
    echo ">>  Database '$DATABASE_NAME' found with ID: $DATABASE_ID"
else
    echo "Error: Database '$DATABASE_NAME' not found."
    exit 1
fi

# Step 5: Save required IDs to the output file
echo ">>  Saving IDs to $OUTPUT_FILE..."
{
    echo "COLLECTION_ID: $COLLECTION_ID"
    echo "DASHBOARD_ID: $DASHBOARD_ID"
    echo "DATABASE_ID: $DATABASE_ID"
    echo "METABASE_URL: $METABASE_URL"
    echo "SESSION_TOKEN: $SESSION_TOKEN"
} > "$OUTPUT_FILE"

# Save the same output to another file with a timestamp
{
    echo "---------------------------------------"
    echo "---------$DASHBOARD_NAME---------------"
    echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
    echo "COLLECTION_ID: $COLLECTION_ID"
    echo "DASHBOARD_ID: $DASHBOARD_ID"
    echo "DATABASE_ID: $DATABASE_ID"
    echo "METABASE_URL: $METABASE_URL"
    echo "SESSION_TOKEN: $SESSION_TOKEN"
} >> "$METADATA_LOG_FILE"

# Check if the file write was successful
if [ $? -eq 0 ]; then
    echo ">>  IDs have been saved successfully to $OUTPUT_FILE."
else
    echo "Error: Failed to save IDs to $OUTPUT_FILE."
    exit 1
fi

echo ">>  [01_create_dashboard.sh] Script executed successfully!"
echo ""
sleep 2

# Call the 02_get_table_data.sh script
$main_dir_path/02_get_table_data.sh "$report_path" "$main_dir_path" "$3"
