#!/bin/bash

## This script retrieves a Metabase dashboard's existing parameters using a session token and dashboard ID,
## appends new filtering parameters to the existing ones, and updates the dashboard with the modified parameters.

BOLD_YELLOW="\033[1;33m"

NC="\033[0m"
echo -e "${BOLD_YELLOW}           :: Update the dashboard with new parameters ::           ${NC}"
echo -e "${NC}"

#External Path 
main_dir=$1
inside_dir_path=$2
new_name=$3

# Check if metadata_file.txt exists
METADATA_FILE="$inside_dir_path/metadata_file.txt"
if [ ! -f "$METADATA_FILE" ]; then
    echo "Error: metadata_file.txt not found."
    exit 1
fi

# Read SESSION_TOKEN and DASHBOARD_ID from metadata_file.txt
SESSION_TOKEN=$(grep "SESSION_TOKEN" "$METADATA_FILE" | cut -d ' ' -f 2)
DASHBOARD_ID=$(grep "DASHBOARD_ID" "$METADATA_FILE" | cut -d ' ' -f 2)
METABASE_URL=$(grep "METABASE_URL" "$METADATA_FILE" | cut -d ' ' -f 2)

# Parameter details to add
PARAMETER_JSON='
                  {
                    "name": "Select State",
                    "slug": "select_state",
                    "id": "c32c8fc5",
                    "type": "string/=",
                    "sectionId": "location",
                    "isMultiSelect": false
                  },
                  {
                    "slug": "select_district",
                    "filteringParameters": [
                      "c32c8fc5"
                    ],
                    "name": "Select District",
                    "isMultiSelect": false,
                    "type": "string/=",
                    "sectionId": "location",
                    "id": "74a10335"
                  },
                  {
                    "slug": "select_program_name",
                    "filteringParameters": [
                      "c32c8fc5",
                      "74a10335"
                    ],
                    "name": "Select Program Name",
                    "isMultiSelect": false,
                    "type": "string/=",
                    "sectionId": "string",
                    "id": "8c7d86ea"
                  }
                '

# Get the existing dashboard details
dashboard_response=$(curl --silent --location --request GET "$METABASE_URL/api/dashboard/$DASHBOARD_ID" \
--header "Content-Type: application/json" \
--header "X-Metabase-Session: $SESSION_TOKEN")

# Extract the current parameters from the dashboard
current_parameters=$(echo "$dashboard_response" | jq -r '.parameters')

# Add the new parameter to the dashboard's existing parameters
updated_parameters=$(echo "$current_parameters" | jq -r ". + [$PARAMETER_JSON]")

# Update the dashboard with the new parameters
update_response=$(curl --silent --location --request PUT "$METABASE_URL/api/dashboard/$DASHBOARD_ID" \
--header "Content-Type: application/json" \
--header "X-Metabase-Session: $SESSION_TOKEN" \
--data-raw "{
    \"parameters\": $updated_parameters
}")

# Check if the dashboard was updated successfully
if echo "$update_response" | grep -q '"parameters"'; then
    echo ">>  Dashboard parameters updated successfully."
else
    echo "Error: Failed to update dashboard parameters. Response: $update_response"
    exit 1
fi

echo ">>  [05_add_parameters.sh] Script executed successfully!"
