#!/bin/bash

user_input=$1

CONFIG_FILE="./config.env"

if [ -f "$CONFIG_FILE" ]; then
    echo "Loading environment variables from $CONFIG_FILE..."
    set -o allexport  # Enable automatic export of variables
    source "$CONFIG_FILE"
    set +o allexport  # Disable automatic export
else
    echo "Warning: $CONFIG_FILE not found. Proceeding with script arguments."
fi

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" >> setup_log.txt
}

# Step 1: Download Docker Compose file
log "Downloading Docker Compose file..."
curl -OJL https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/docker-compose.yml
log "Docker Compose file downloaded."

# Step 2: Download environment files
log "Downloading required files..."
curl -L \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/config.env \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/config_files/base-config.conf \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/config_files/metabase-dashboard.conf \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/config_files/project-stream.conf \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/config_files/application.conf \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/create-table.sh \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/deploy-flink-job.sh \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/dummy-event-data.json \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/submit-jobs.sh \
    -O https://raw.githubusercontent.com/prashanthShiksha/data-pipeline/dev-deploy/Documentation/Docker-setup/docker-compose.yml
log "All files downloaded."

# Step 3: Move conf files into the config_files folder
log "Moving conf files into the config_files folder..."
mkdir -p config_files
mv base-config.conf config_files/
mv metabase-dashboard.conf config_files/
mv project-stream.conf config_files/
mv application.conf config_files/
log "Conf files moved successfully."

# Step 4: Make the scripts executable
log "Making shell scripts executable..."
chmod +x ./create-table.sh
chmod +x ./deploy-flink-job.sh
chmod +x ./submit-jobs.sh
log "Made shell scripts executable."

echo "If you need to make any changes in the config.env file or any other files, please do so now."
read -p "can we move ahead for further processing? (yes/no): " user_input

if [ "$user_input" == "yes" ]; then
    log "Running docker-compose-up.sh script..."
    sudo docker-compose --env-file ./config.env up -d
    log "docker-compose-up.sh script executed."
else
    echo "Please verify the services and run the script again."
    exit 1
fi

# Step 5: Prompt user to verify services
echo "Please verify the following services are running:"
echo "1. Metabase UI (http://localhost:3000)"
echo "2. Flink UI (http://localhost:8081)"
echo "3. PG Admin (http://localhost:5050)"
read -p "Have you verified the services? (yes/no): " user_input

if [ "$user_input" == "yes" ]; then
    log "Creating table in the database..."
    sudo docker exec -it elevate-data /app/Documentation/Docker-setup/create-table.sh "$PROJECT_DB" "$POSTGRES_USER" "$POSTGRES_PASSWORD" "$POSTGRES_HOST" "$POSTGRES_PORT" "$PROJECT_ENV"
    log "Table created successfully."
else
    echo "Please verify the services and run the script again."
    exit 1
fi

# Step 6: Prompt user to set up Metabase and PG Admin
while true; do
    echo "Please go to the Metabase UI and set up the super admin account and database connection."
    echo "Also, set up the server for PG Admin."
    echo "1. Metabase UI (http://localhost:3000)"
    echo "2. PG Admin (http://localhost:5050)"
    read -p "Have you completed the setup? (yes/no): " user_input

    if [ "$user_input" == "yes" ]; then
        log "Creating Kafka topics and submitting Flink job..."
        ./deploy-flink-job.sh
        log "Kafka topics created and Flink job submitted successfully."
        break
    elif [ "$user_input" == "no" ]; then
        echo "Please complete the setup and run the script again."
        exit 1
    else
        echo "Please enter a valid command: yes or no."
    fi
done

# step 7 : submit the dummy-event-data
while true; do
    read -p "Do you want to submit the dummy-data? (yes/no): " user_input

    if [ "$user_input" == "yes" ]; then
      docker exec -i kafka /usr/bin/kafka-console-producer --broker-list kafka:9092 --topic sl-improvement-project-submission-dev < ./dummy-event-data.json
      break
    elif [ "$user_input" == "no" ]; then
        echo "Please complete the setup and run the script again."
        exit 1
    else
        echo "Please enter a valid command: yes or no."
    fi
done

# Step 7 : create user via csv
while true; do
    read -p "Do you want to create user-via-csv? (yes/no): " user_input

    if [ "$user_input" == "yes" ]; then
      docker exec -it elevate-data mkdir -p /app/logs
      docker exec -it elevate-data mkdir -p /app/csv
      sudo docker exec -it elevate-data /bin/bash -c "nohup java -jar /app/metabase-jobs/users-via-csv/target/users-via-csv-1.0.0.jar >> /app/logs/MetabaseUserUploadLogs.logs 2>&1 &"
      echo "Waiting for 10 seconds to allow the service to start..."
      sleep 10
      docker exec -it elevate-data bash -c "ps -ef | grep users-via-csv"
      docker exec -it elevate-data bash -c "curl --location 'http://localhost:8080/api/csv/upload' \
                                            --header 'Authorization: 8f934c7a-71b1-4ec9-98c2-d472cd9e5f1a' \
                                            --form 'file=@"/app/Documentation/Docker-setup/user_data.csv"'"
      break
    elif [ "$user_input" == "no" ]; then
        echo "Please complete the setup and run the script again."
        exit 1
    else
        echo "Please enter a valid command: yes or no."
    fi
done