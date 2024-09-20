#!/bin/bash

## This script updates JSON files within specified subdirectories by replacing database and collection_id
## values in the dataset_query object based on IDs read from a configuration file.
## It ensures that the IDs are numeric and processes each subdirectory if it exists.

BOLD_YELLOW="\033[1;33m"
NC="\033[0m"
echo -e "${BOLD_YELLOW}       :: Updating the all the request body in json files ::        ${NC}"
echo -e "${NC}"

# Directory setup
JSON_DIR="./json"
BIG_NUMBER_DIR="$JSON_DIR/big-number"
GRAPH_DIR="$JSON_DIR/graph"
TABLE_DIR="$JSON_DIR/table"

# Check if metadata_file.txt exists
METADATA_FILE="./metadata_file.txt"
if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: metadata_file.txt not found."
    exit 1
fi

# Read and clean COLLECTION_ID and DATABASE_ID from metadata_file.txt
COLLECTION_ID=$(grep "COLLECTION_ID" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')
DATABASE_ID=$(grep "DATABASE_ID" "$METADATA_FILE" | cut -d':' -f2 | tr -d '[:space:]')

# Print the values for debugging
echo ">>  COLLECTION_ID: $COLLECTION_ID"
echo ">>  DATABASE_ID: $DATABASE_ID"

# Ensure COLLECTION_ID and DATABASE_ID are numbers
if ! [[ "$COLLECTION_ID" =~ ^[0-9]+$ ]]; then
    echo "Error: COLLECTION_ID is not a valid number."
    exit 1
fi

if ! [[ "$DATABASE_ID" =~ ^[0-9]+$ ]]; then
    echo "Error: DATABASE_ID is not a valid number."
    exit 1
fi

# Convert to numbers (for use in jq)
COLLECTION_ID_NUM=$((COLLECTION_ID))
DATABASE_ID_NUM=$((DATABASE_ID))

# Function to update JSON files within dataset_query and questionCard
update_json_files() {
    DIR="$1"
    if [ -d "$DIR" ]; then
        for FILE in "$DIR"/*.json; do
            if [ -f "$FILE" ]; then
                echo "    --- Updating $FILE"
                # Validate JSON file before processing
                if jq empty "$FILE" > /dev/null 2>&1; then
                    # Update questionCard's collection_id and dataset_query's database
                    jq --argjson db "$DATABASE_ID_NUM" --argjson coll "$COLLECTION_ID_NUM" \
                    '.questionCard.collection_id = $coll | .questionCard.dataset_query.database = $db' \
                    "$FILE" > tmp.json && mv tmp.json "$FILE"
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

echo ">>  JSON files have been updated with DATABASE_ID=$DATABASE_ID and COLLECTION_ID=$COLLECTION_ID inside questionCard."
echo ">>  [02_update_metabase_json_ids.sh] Script executed successfully!"
echo ""
echo ""
sleep 2

# Call the 03_add_question_cards_to_dashboard.sh script
./03_add_question_cards_to_dashboard.sh
