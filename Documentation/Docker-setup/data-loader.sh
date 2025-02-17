#!/bin/bash

# Database connection parameters
DB_NAME="$1"
DB_USER="$2"
DB_PASSWORD="$3"
DB_HOST="$4"
DB_PORT="$5"
ENV="$6"
TABLE_NAME="${ENV}_report_config"

# Export PostgreSQL password for psql
export PGPASSWORD="$DB_PASSWORD"

# Create table function
create_table() {
    psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -p "$DB_PORT" -c "
    CREATE TABLE IF NOT EXISTS $TABLE_NAME (
        id SERIAL PRIMARY KEY,
        dashboard_name TEXT NOT NULL,
        report_name TEXT NOT NULL,
        question_type TEXT NOT NULL,
        config JSON NOT NULL
    );"
}

# Process folders function
process_folders() {
    local main_folder=$1

    for main_folder_name in "$main_folder"/*; do
        if [[ -d "$main_folder_name" ]]; then
            local dashboard_name
            dashboard_name=$(basename "$main_folder_name")

            for folder_name in "$main_folder_name"/*; do
                if [[ -d "$folder_name" ]]; then
                    local report_name
                    report_name=$(basename "$folder_name")
                    local json_folder_path="$folder_name/json"

                    if [[ ! -d "$json_folder_path" ]]; then
                        echo "No 'json' folder found in $folder_name"
                        continue
                    fi

                    for query_type_path in "$json_folder_path"/*; do
                        if [[ -d "$query_type_path" ]]; then
                            local query_type
                            query_type=$(basename "$query_type_path")

                            for json_file in "$query_type_path"/*.json; do
                                if [[ -f "$json_file" && "$json_file" == *.json ]]; then
                                    # Read JSON content safely
                                    config=$(cat "$json_file" | jq -c .)

                                    # Debugging logs
                                    echo "Inserting into table: $TABLE_NAME"
                                    echo "Dashboard Name: $dashboard_name, Report Name: $report_name, Query Type: $query_type"

                                    # Insert into PostgreSQL using variables safely
                                    psql -d "$DB_NAME" -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -v table_name="$TABLE_NAME" -v dashboard="$dashboard_name" -v report="$report_name" -v querytype="$query_type" -v jsonconfig="$config" -c "
                                        INSERT INTO "$TABLE_NAME" (dashboard_name, report_name, question_type, config)
                                        VALUES ('$dashboard_name', '$report_name', '$query_type', \$\$${config}\$\$);
                                    "
                                fi
                            done
                        fi
                    done
                fi
            done
        fi
    done

    echo "All data processed successfully."
}

# Main folder path
MAIN_FOLDER="/app/metabase-jobs/config-data-loader/projectJson"

# Create the table and process folders
create_table
process_folders "$MAIN_FOLDER"

# Unset password for security
unset PGPASSWORD
