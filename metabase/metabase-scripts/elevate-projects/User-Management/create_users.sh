#!/bin/bash

# Print the current working directory
echo "Current directory: $(pwd)"

# Metabase configuration
METABASE_URL="http://localhost:3000"
METABASE_USERNAME=""
METABASE_PASSWORD=""
USER_INFO_FILE="user-info.json"

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

# Read user information from JSON file
if [[ ! -f "$USER_INFO_FILE" ]]; then
    echo "Error: $USER_INFO_FILE not found!"
    exit 1
fi

# Get the list of existing users
existing_users_response=$(curl --silent --location --request GET "$METABASE_URL/api/user" \
    --header "X-Metabase-Session: $SESSION_TOKEN")

# Process each user in user-info.json
jq -c '.[]' "$USER_INFO_FILE" | while read -r user_info; do

    # Print the entire user object for debugging
    echo ""
    echo "Processing user: $user_info"
    email=$(echo "$user_info" | jq -r .email)
    first_name=$(echo "$user_info" | jq -r .first_name)
    last_name=$(echo "$user_info" | jq -r .last_name)
    password=$(echo "$user_info" | jq -r .password)
    state_name=$(echo "$user_info" | jq -r .state)
    district_name=$(echo "$user_info" | jq -r .district)
    user_roles=$(echo "$user_info" | jq -r .role)
    echo ""

    # Check if user already exists in the existing users list
    existing_user_id=$(echo "$existing_users_response" | jq -r --arg email "$email" '.data[] | select(.email == $email) | .id')

    if [[ -n "$existing_user_id" ]]; then
        # User exists, print an error and their user ID
        echo "User $email already exists with ID: $existing_user_id."
        ./role_mapping.sh "$existing_user_id" "$state_name" "$district_name" "$user_roles"
    else
        # Create user if they do not exist
        create_user_response=$(curl --silent --location --request POST "$METABASE_URL/api/user" \
            --header "Content-Type: application/json" \
            --header "X-Metabase-Session: $SESSION_TOKEN" \
            --data-raw "{
                \"first_name\": \"$first_name\",
                \"last_name\": \"$last_name\",
                \"email\": \"$email\",
                \"password\": \"$password\"
            }")

        # Check if user creation was successful
        if echo "$create_user_response" | jq -e '.id' > /dev/null; then
            created_user_id=$(echo "$create_user_response" | jq -r '.id')
            echo "User $(echo "$user_info" | jq -r .email) created successfully with ID: $created_user_id."
            ./role_mapping.sh "$created_user_id" "$state_name" "$district_name" "$user_roles"
        else
            echo "Failed to create user $(echo "$user_info" | jq -r .email): $create_user_response"
        fi
    fi
done
