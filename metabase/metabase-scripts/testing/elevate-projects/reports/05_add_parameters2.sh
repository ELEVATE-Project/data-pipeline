#!/bin/bash

## This script retrieves a Metabase dashboard's existing parameters using a session token and dashboard ID,
## appends new filtering parameters to the existing ones, and updates the dashboard with the modified parameters.

# ANSI escape codes for colors
BOLD_YELLOW="\033[1;33m"
NC="\033[0m"

echo -e "${BOLD_YELLOW}           :: Update the dashboard with new parameters ::           ${NC}"
echo -e "${NC}"

# Check input arguments
if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <report_path> <main_dir_path> <report_name> <output_file> [state_name/program_name]"
  exit 1
fi

#External Path 
report_path="$1"
main_dir_path="$2"
report_name="$3"
name_value="$4"


# Function to replace placeholders in the JSON file
generate_json_from_template() {
  local template_file=$1
  local state_name=$2
  local program_name=$3

  # Read the template JSON file and replace placeholders, storing the result in a variable
  json_output=$(sed -e "s/{{state_name}}/$state_name/g" -e "s/{{program_name}}/$program_name/g" "$template_file")
}

case "$report_name" in
  "state report")
    if [[ -z "$name_value" ]]; then
      echo "State name is required for state report."
      exit 1
    fi
    template_file="$main_dir_path/parameter-json/state-report.json"
    generate_json_from_template "$template_file" "$name_value" ""
    ;;
  "super admin report")
    template_file="$main_dir_path/parameter-json/super-admin-report.json"
    generate_json_from_template "$template_file" "" ""
    ;;
  "program report")
    if [[ -z "$name_value" ]]; then
      echo "Program name is required for program report."
      exit 1
    fi
    template_file="$main_dir_path/parameter-json/program-report.json"
    generate_json_from_template "$template_file" "" "$name_value"
    ;;
  *)
    echo "Invalid report name. Valid options are: state report, super admin report, program report."
    exit 1
    ;;
esac

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

# Parameter details to add
PARAMETER_JSON=$json_output

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
