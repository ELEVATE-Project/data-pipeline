#!/bin/bash

## This script updates JSON files within specified subdirectories by replacing database and collection_id
## values in the dataset_query object based on IDs read from a configuration file.
## It ensures that the IDs are numeric and processes each subdirectory if it exists.

# ANSI escape codes for colors
BOLD_YELLOW="\033[1;33m"
NC="\033[0m"

echo -e "${BOLD_YELLOW}       :: Updating all the request bodies in JSON files ::        ${NC}"
echo -e "${NC}"

#External Path 
report_path=$1

# Directory setup
JSON_DIR="$report_path/json"
BIG_NUMBER_DIR="$JSON_DIR/big-number"
GRAPH_DIR="$JSON_DIR/graph"
TABLE_DIR="$JSON_DIR/table"
HEADING_DIR="$JSON_DIR/heading"

main_dir_path=$2
echo "$main_dir_path"
# Check if metadata_file.txt exists
METADATA_FILE="$main_dir_path/metadata_file.txt"

if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: metadata_file.txt not found."
    exit 1
fi

# Read and clean COLLECTION_ID and DATABASE_ID from metadata_file.txt
COLLECTION_ID=$(grep "COLLECTION_ID" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')
DATABASE_ID=$(grep "DATABASE_ID" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')
DASHBOARD_ID=$(grep "DASHBOARD_ID" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')

# Extract the required IDs from the metadata file
STATENAME_ID=$(grep "projects - statename" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')
DISTRICTNAME_ID=$(grep "projects - districtname" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')
PROGRAMNAME_ID=$(grep "solutions - programname" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')

# Convert to integers
STATENAME_ID_NUM=$((STATENAME_ID))
DISTRICTNAME_ID_NUM=$((DISTRICTNAME_ID))
PROGRAMNAME_ID_NUM=$((PROGRAMNAME_ID))

# Print the values for debugging
echo ">>  COLLECTION_ID: $COLLECTION_ID"
echo ">>  DATABASE_ID: $DATABASE_ID"
echo ">>  DASHBOARD_ID: $DASHBOARD_ID"

# Ensure COLLECTION_ID and DATABASE_ID are numbers
if ! [[ "$COLLECTION_ID" =~ ^[0-9]+$ ]]; then
    echo "Error: COLLECTION_ID is not a valid number."
    exit 1
fi

if ! [[ "$DATABASE_ID" =~ ^[0-9]+$ ]]; then
    echo "Error: DATABASE_ID is not a valid number."
    exit 1
fi

if ! [[ "$DASHBOARD_ID" =~ ^[0-9]+$ ]]; then
    echo "Error: DASHBOARD_ID is not a valid number."
    exit 1
fi

# Convert to numbers (for use in jq)
COLLECTION_ID_NUM=$((COLLECTION_ID))
DATABASE_ID_NUM=$((DATABASE_ID))
DASHBOARD_ID_NUM=$((DASHBOARD_ID))

# Function to update JSON files within dataset_query and questionCard
update_json_files() {
    DIR="$1"
    if [ -d "$DIR" ]; then
        for FILE in "$DIR"/*.json; do
            if [ -f "$FILE" ]; then
                echo "    --- Updating $FILE"
                # Validate JSON file before processing
                if jq empty "$FILE" > /dev/null 2>&1; then
                    # Conditionally update JSON based on the presence of keys
                    jq --argjson db "$DATABASE_ID_NUM" --argjson coll "$COLLECTION_ID_NUM" --argjson dash "$DASHBOARD_ID_NUM" \
                       --argjson statename "$STATENAME_ID_NUM" --argjson districtname "$DISTRICTNAME_ID_NUM" --argjson programname "$PROGRAMNAME_ID_NUM" \
                    '
                    if .questionCard.collection_id != null then
                        .questionCard.collection_id = $coll
                    else
                        .
                    end |
                    if .questionCard.dashboard_id != null then
                        .questionCard.dashboard_id = $dash
                    else
                        .
                    end |
                    if .questionCard.dataset_query != null then
                        .questionCard.dataset_query.database = $db
                    else
                        .
                    end |
                    if .questionCard.dataset_query.native["template-tags"]["state_param"].dimension[1] != null then
                        .questionCard.dataset_query.native["template-tags"]["state_param"].dimension[1] = $statename
                    else
                        .
                    end |
                    if .questionCard.dataset_query.native["template-tags"]["district_param"].dimension[1] != null then
                        .questionCard.dataset_query.native["template-tags"]["district_param"].dimension[1] = $districtname
                    else
                        .
                    end |
                    if .questionCard.dataset_query.native["template-tags"]["program_param"].dimension[1] != null then
                        .questionCard.dataset_query.native["template-tags"]["program_param"].dimension[1] = $programname
                    else
                        .
                    end
                    ' "$FILE" > tmp.json && mv tmp.json "$FILE"
                else
                    echo "Warning: File '$FILE' is not valid JSON. Skipping..."
                fi
            fi
        done
    else
        echo "**  Warning: Directory '$DIR' not found. Skipping..."
    fi
}

# Process each subdirectory (skip missing ones)
update_json_files "$BIG_NUMBER_DIR"
update_json_files "$GRAPH_DIR"
update_json_files "$TABLE_DIR"
update_json_files "$HEADING_DIR"

echo ">>  JSON files have been updated with DATABASE_ID=$DATABASE_ID and COLLECTION_ID=$COLLECTION_ID and DASHBOARD_ID=$DASHBOARD_ID inside questionCard."
echo ">>  [03_update_json_files.sh] Script executed successfully!"
echo ""
sleep 2

# Call the 04_add_question_cards.sh script
$main_dir_path/04_add_question_cards.sh $report_path $main_dir_path