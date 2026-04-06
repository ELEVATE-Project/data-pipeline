#!/bin/bash

# Install postgresql-client
echo "Installing postgresql-client..."
if [ -f /etc/alpine-release ]; then
    apk update && apk add postgresql-client
elif [ -f /etc/debian_version ]; then
    apt-get update && apt-get install -y postgresql-client
else
    echo "Unsupported OS for automatic installation. Please install postgresql-client manually."
fi

# Function to update schema
update_schema() {
    echo "Waiting for Metabase to initialize and create tables..."
    # Wait for a bit to ensure Postgres is up and Metabase has started migration
    sleep 30

    MAX_RETRIES=5
    COUNTER=0

    while [ $COUNTER -lt $MAX_RETRIES ]; do
        # check if collection table exists
        if PGPASSWORD="$MB_DB_PASS" psql -h "$MB_DB_HOST" -U "$MB_DB_USER" -d "$MB_DB_DBNAME" -c "\dt collection" | grep -q "collection"; then
            echo "Collection table found. Checking 'slug' column type..."
            
            # Check current data type of slug column
            CURRENT_TYPE=$(PGPASSWORD="$MB_DB_PASS" psql -h "$MB_DB_HOST" -U "$MB_DB_USER" -d "$MB_DB_DBNAME" -t -c "SELECT data_type FROM information_schema.columns WHERE table_name = 'collection' AND column_name = 'slug';" | tr -d '[:space:]')

            if [ "$CURRENT_TYPE" = "text" ]; then
                echo "'slug' column is already TEXT. Skipping update."
                break
            else
                echo "Current type is '$CURRENT_TYPE'. Updating to TEXT..."
                PGPASSWORD="$MB_DB_PASS" psql -h "$MB_DB_HOST" -U "$MB_DB_USER" -d "$MB_DB_DBNAME" -c "ALTER TABLE collection ALTER COLUMN slug TYPE TEXT;"
                if [ $? -eq 0 ]; then
                    echo "Schema update successful: 'slug' column in 'collection' table changed to TEXT."
                    break
                else
                    echo "Schema update failed. Retrying..."
                fi
            fi
        else
            echo "Collection table not found yet. Waiting..."
        fi
        let COUNTER=COUNTER+1
    done

    if [ $COUNTER -eq $MAX_RETRIES ]; then
        echo "Timeout waiting for collection table or schema update."
    fi
}

# Run the update function in the background
update_schema &

# Start Metabase (Original Entrypoint)
echo "Starting Metabase..."
/app/run_metabase.sh
