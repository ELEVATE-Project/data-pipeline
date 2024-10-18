#!/bin/bash

## This script automates the process of creating and adding question cards to a Metabase dashboard.
## It reads the Url, session token and dashboard ID from a file, processes JSON files from specified directories,
## creates question cards via API calls, and updates the dashboard with the new cards.

# ANSI escape codes for colors
BOLD_YELLOW="\033[1;33m"
BOLD_GREEN="\033[1;32m"
NC="\033[0m"

echo -e "${BOLD_YELLOW}     :: Creating question card and adding them to dashboard ::      ${NC}"
echo -e "${NC}"

#External Path 
report_path=$1
main_dir_path=$2 

# Directory setup
JSON_DIR="$report_path/json"
BIG_NUMBER_DIR="$JSON_DIR/big-number"
GRAPH_DIR="$JSON_DIR/graph"
TABLE_DIR="$JSON_DIR/table"
HEADING_DIR="$JSON_DIR/heading"

# Check if metadata_file.txt exists
METADATA_FILE="$main_dir_path/metadata_file.txt"
if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: metadata_file.txt not found."
    exit 1
fi

# Read SESSION_TOKEN and DASHBOARD_ID from metadata_file.txt
SESSION_TOKEN=$(grep "SESSION_TOKEN" "$METADATA_FILE" | cut -d ' ' -f 2)
DASHBOARD_ID=$(grep "DASHBOARD_ID" "$METADATA_FILE" | cut -d ' ' -f 2)
METABASE_URL=$(grep "METABASE_URL" "$METADATA_FILE" | cut -d ' ' -f 2)

# Check if METABASE_URL, SESSION_TOKEN and DASHBOARD_ID were retrieved successfully
if [ -z "$METABASE_URL" ]; then
    echo "Error: DASHBOARD_ID not found in $METADATA_FILE."
    exit 1
else
    echo ">> Metabase URL read successfully: $METABASE_URL"
fi

if [ -z "$SESSION_TOKEN" ]; then
    echo "Error: SESSION_TOKEN not found in $METADATA_FILE."
    exit 1
else
    echo ">> Session Token read successfully: $SESSION_TOKEN"
fi

if [ -z "$DASHBOARD_ID" ]; then
    echo "Error: DASHBOARD_ID not found in $METADATA_FILE."
    exit 1
else
    echo ">> Dashboard ID read successfully: $DASHBOARD_ID"
fi

# Function to create question card, update card_id, and append to dashcards
process_json_files() {
    DIR="$1"
    if [ -d "$DIR" ]; then
        for JSON_FILE in "$DIR"/*.json; do
            if [ -f "$JSON_FILE" ]; then
                CHART_NAME=$(jq '.questionCard.name' "$JSON_FILE")
                echo -e "${BOLD_GREEN}   --- Started Processing For The Chart: $CHART_NAME ${NC}"

                # Validate JSON file before processing
                if jq empty "$JSON_FILE" > /dev/null 2>&1; then

                    # Get the request body for the question card creation
                    REQUEST_BODY=$(jq '.questionCard' "$JSON_FILE")

                    # Make the API call to create the question card
                    response=$(curl --silent --location --request POST "$METABASE_URL/api/card" \
                        --header "Content-Type: application/json" \
                        --header "X-Metabase-Session: $SESSION_TOKEN" \
                        --data "$REQUEST_BODY")

                    # Extract the card_id from the response
                    CARD_ID=$(echo "$response" | jq '.id')

                    # Check if the card_id was extracted successfully
                    if [ -z "$CARD_ID" ]; then
                        echo "Error: Failed to extract card_id from the API response for $CHART_NAME."
                        continue
                    else
                        echo "   >> Successfully created question card with card_id: $CARD_ID for $CHART_NAME"
                    fi

                    # Update the dashCards key in the JSON file with the new card_id
                    jq --argjson card_id "$CARD_ID" '.dashCards.card_id = $card_id | .dashCards.parameter_mappings |= map(.card_id = $card_id)' "$JSON_FILE" > "${JSON_FILE}.tmp" && mv "${JSON_FILE}.tmp" "$JSON_FILE"

                    # Append the new dashCard to the dashboard
                    append_dashcard_to_dashboard "$JSON_FILE"

                else
                    echo "Warning: File '$JSON_FILE' is not valid JSON. Skipping..."
                fi
            fi
        done
    else
        echo "**  Warning: Directory '$DIR' not found. Skipping..."
    fi
}

# Function to append question to the dashboard
append_dashcard_to_dashboard() {
    JSON_FILE="$1"

    # Get the dashCard detail to load and ensure that dashcard is an array (wrap in array if it's an object)
    new_dashcards=$(jq '.dashCards' "$JSON_FILE")
    new_dashcards=$(jq -c 'if type == "object" then [.] else . end' <<< "$new_dashcards")

    # Make the GET request to fetch the dashboard
    dashboard_response=$(curl --silent --location --request GET "$METABASE_URL/api/dashboard/$DASHBOARD_ID" \
        --header "Content-Type: application/json" \
        --header "X-Metabase-Session: $SESSION_TOKEN")

    # Process the response and extract the existing dashcards
    existing_dashcards=$(echo "$dashboard_response" | jq '.dashcards')

    # Check if dashcards were retrieved successfully
    if [ -z "$existing_dashcards" ]; then
        echo "Error: Failed to retrieve dashcards from the dashboard response."
        exit 1
    else
        echo "   >> Successfully retrieved existing dashcards."
    fi

    # Combine existing and new dashcards
    updated_dashcards=$(jq --argjson existing "$existing_dashcards" --argjson new "$new_dashcards" '. |= $existing + $new' <<< '[]')

    # Check if dashcards were combined successfully
    if [ -z "$updated_dashcards" ]; then
        echo "Error: Failed to combine existing and new dashcards."
        exit 1
    else
        echo "   >> Successfully combined existing dashcards with new dashcard."
    fi

    # # Create request body for the update
    # UPDATE_REQUEST_BODY=$(jq --argjson dashcards "$updated_dashcards" '{dashcards: $dashcards}' <<< '{}')

    # # Make the PUT request to update the dashboard
    # update_response=$(curl --silent --location --request PUT "$METABASE_URL/api/dashboard/$DASHBOARD_ID" \
    # --header "Content-Type: application/json" \
    # --header "X-Metabase-Session: $SESSION_TOKEN" \
    # --data "$UPDATE_REQUEST_BODY")

    # # Check if the update was successful
    # if [[ "$update_response" == *"error"* ]]; then
    #     echo "Error: Failed to update the dashboard."
    #     exit 1
    # else
    #     echo "   >> Successfully created question card and added it to dashboard."
    # fi
    # Create request body for the update and save it to a temporary file
    TEMP_UPDATE_REQUEST_BODY=$(mktemp)
    jq --argjson dashcards "$updated_dashcards" '{dashcards: $dashcards}' <<< '{}' > "$TEMP_UPDATE_REQUEST_BODY"

    # Make the PUT request to update the dashboard using the temporary file
    update_response=$(curl --silent --location --request PUT "$METABASE_URL/api/dashboard/$DASHBOARD_ID" \
        --header "Content-Type: application/json" \
        --header "X-Metabase-Session: $SESSION_TOKEN" \
        --data @"$TEMP_UPDATE_REQUEST_BODY")

    # Clean up the temporary file after the request
    rm "$TEMP_UPDATE_REQUEST_BODY"

    # Check if the update was successful
    if [[ "$update_response" == *"error"* ]]; then
        echo "Error: Failed to update the dashboard."
        exit 1
    else
        echo "   >> Successfully created question card and added it to dashboard."
    fi


    echo ""
    sleep 1 # Add a delay to avoid overwhelming the API with requests
}

# Process each subdirectory (skip missing ones)
process_json_files "$BIG_NUMBER_DIR"
process_json_files "$GRAPH_DIR"
process_json_files "$TABLE_DIR"
process_json_files "$HEADING_DIR"

echo ">>  [04_add_question_cards.sh] Script executed successfully!"
echo ""
sleep 2

# Call the 05_add_parameters.sh script
# $main_dir_path/05_add_parameters.sh $report_path $main_dir_path