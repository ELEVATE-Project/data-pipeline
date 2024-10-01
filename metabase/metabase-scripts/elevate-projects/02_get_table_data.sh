#!/bin/bash

## This script retrieves database metadata from a Metabase instance using
## a session token and database ID specified in a metadata file, extracts table and field names along with their IDs,
## and appends this information to the same metadata file.

BOLD_YELLOW="\033[1;33m"
BOLD_GREEN="\033[1;32m"

NC="\033[0m"
echo -e "${BOLD_YELLOW}          :: Get the required table data from Metabase ::              ${NC}"
echo -e "${NC}"

#External Path 
report_path=$1
# Check if metadata_file.txt exists
METADATA_FILE="$report_path/metadata_file.txt"
if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: metadata_file.txt not found."
    exit 1
fi

# Read SESSION_TOKEN and DATABASE_ID from metadata_file.txt
SESSION_TOKEN=$(grep "SESSION_TOKEN" "$METADATA_FILE" | cut -d ' ' -f 2)
DATABASE_ID=$(grep "DATABASE_ID" "$METADATA_FILE" | cut -d ' ' -f 2)
METABASE_URL=$(grep "METABASE_URL" "$METADATA_FILE" | cut -d ' ' -f 2)

# Function to get database metadata
get_database_metadata() {
    local response=$(curl -s -X GET "$METABASE_URL/api/database/$DATABASE_ID/metadata" \
        -H "Content-Type: application/json" \
        -H "X-Metabase-Session: $SESSION_TOKEN")

    echo "$response"
}

# Function to extract table names and IDs
extract_tables() {
    local metadata="$1"
    echo "$metadata" | jq -r '.tables[] | "\(.name): \(.id)"'
}

# Function to extract table names and field IDs
extract_tables_and_fields() {
    local metadata="$1"
    echo "$metadata" | jq -r '.tables[] | .name as $table_name | .fields[] | "\($table_name) - \(.name): \(.id)"'
}

# Main script
echo ">>  Extracting table names and IDs..."
metadata=$(get_database_metadata)

# Append new data to metadata_file.txt
{
    echo "------------ Table Name and ID's ------------"
    if [ -n "$metadata" ]; then
        extract_tables "$metadata"
    else
        echo "Failed to retrieve metadata."
    fi

    echo "------------ Field Names and ID's ------------"
    if [ -n "$metadata" ]; then
        extract_tables_and_fields "$metadata"
    else
        echo "Failed to retrieve metadata."
    fi
} >> "$METADATA_FILE"

echo ">>  Saved the Ids to Metadata File"

echo ">>  [02_get_table_data.sh] Script executed successfully!"
echo ""
echo ""
sleep 2

# Call the 03_update_json_files.sh script
./03_update_json_files.sh $report_path
