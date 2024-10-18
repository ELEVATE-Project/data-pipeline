#!/bin/bash

echo ""
echo "INSIDE STATE USER MAPPING"
user_id="$1"
state_name="$2"
district_name="$3"

# Metabase configuration
METABASE_URL="http://localhost:3000"
METABASE_USERNAME=""
METABASE_PASSWORD=""

# Authenticate and get Metabase session token
session_response=$(curl --silent --location --request POST "$METABASE_URL/api/session" \
--header "Content-Type: application/json" \
--data-raw "{
  \"username\": \"$METABASE_USERNAME\",
  \"password\": \"$METABASE_PASSWORD\"
}")

# Extract the session token using jq
SESSION_TOKEN=$(echo "$session_response" | jq -r '.id')

# Check if authentication was successful
if [[ -z "$SESSION_TOKEN" ]]; then
    echo "Error: Unable to authenticate to Metabase."
    exit 1
fi

# Call the API to get all groups
groups_response=$(curl --silent --location --request GET "$METABASE_URL/api/permissions/group" \
    --header "X-Metabase-Session: $SESSION_TOKEN")

#echo "Groups Response: $groups_response"  # Print the entire response for debugging

# Check if the groups response was successful
if [[ -z "$groups_response" || $(echo "$groups_response" | jq 'type') != '"array"' ]]; then
    echo "Error retrieving groups: Invalid response received."
    exit 1
fi


# Create a JSON variable with keys group_id and group_name
existing_groups=$(echo "$groups_response" | jq -c '[.[] | {id: .id, name: .name}]')
echo "Existing Groups: $existing_groups"

# Check for State User group
state_user_groups=$(echo "$existing_groups" | jq '[.[] | select(.name == "Karnataka State User")]')
echo "state_user_groups : $state_user_groups"

# Count the number of State User groups
state_user_count=$(echo "$state_user_groups" | jq 'length')
echo "state_user_count : $state_user_count"

# Check if there is exactly one State User group
if [[ $state_user_count -eq 1 ]]; then
    state_user_group_id=$(echo "$state_user_groups" | jq -r '.[0].id')
    echo "Karnataka State User Group ID: $state_user_group_id"
else
    echo "Error: Expected exactly one 'Karnataka State User' group, but found $state_user_count."
    exit 1
fi


add_user_response=$(curl --silent --location --request POST "$METABASE_URL/api/permissions/membership" \
    --header "X-Metabase-Session: $SESSION_TOKEN" \
    --header "Content-Type: application/json" \
    --data-raw "{
        \"user_id\": $user_id,
        \"group_id\": $state_user_group_id
    }" --write-out "%{http_code}" --output /dev/null)
if [[ "$add_user_response" -ne 200 ]]; then
    echo "User $user_id is already mapped to the Karnataka State User group or another error occurred. HTTP Status Code: $add_user_response"
else
    echo "User $user_id successfully added to Karnataka State User group."
fi