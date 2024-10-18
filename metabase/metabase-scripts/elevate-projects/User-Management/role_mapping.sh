#!/bin/bash

echo ""
echo "INSIDE ROLE MAPPING"
user_id="$1"
state_name="$2"
district_name="$3"
user_roles="$4"

# Print the values
echo "Existing User ID: $user_id"
echo "State Name: $state_name"
echo "District Name: $district_name"
echo "User Roles: $user_roles"

# Split user_roles by commas and loop through each role
IFS=',' read -r -a roles_array <<< "$user_roles"

echo ""
echo "Processing User Roles:"
for role in "${roles_array[@]}"; do
    role=$(echo "$role" | xargs)  # Trim leading/trailing whitespace
    echo "User Role: $role"

    if [[ "$role" == "report_admin" ]]; then
        ./report_admin.sh "$user_id" "$state_name" "$district_name"
    elif [[ "$role" == "state_user" ]]; then
        ./state_user.sh "$user_id" "$state_name" "$district_name"
    elif [[ "$role" == "district_user" ]]; then
        ./district_user.sh "$user_id" "$state_name" "$district_name"
    elif [[ "$role" == "program_manager" ]]; then
        ./program_user.sh "$user_id" "$state_name" "$district_name"
    else
        echo "No specific script to run for role: $role"
    fi
done
