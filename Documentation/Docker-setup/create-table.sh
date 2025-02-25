#!/bin/bash

DB_NAME="$1"
DB_USER="$2"
DB_PASSWORD="$3"
DB_HOST="$4"
DB_PORT="$5"
ENV="$6"

METADATA_TABLE="${ENV}_dashboard_metadata"
SOLUTIONS_TABLE="${ENV}_solutions"
PROJECTS_TABLE="${ENV}_projects"
TASKS_TABLE="${ENV}_tasks"
REPORT_CONFIG_TABLE="${ENV}_report_config"
DATABASE_NAME="${ENV}-${DB_NAME}"
MAIN_FOLDER="/app/metabase-jobs/config-data-loader/projectJson"

# Check if required variables are set
if [[ -z "$DB_HOST" || -z "$DB_PORT" || -z "$DB_NAME" || -z "$DB_USER" || -z "$DB_PASSWORD" ]]; then
    echo "Error: One or more required PostgreSQL environment variables are missing."
    exit 1
fi

# Create database if it doesn't exist
echo "Checking if database $DATABASE_NAME exists..."
DB_EXISTS=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -tc "SELECT 1 FROM pg_database WHERE datname = '$DATABASE_NAME';" | tr -d '[:space:]')

if [[ "$DB_EXISTS" != "1" ]]; then
    echo "Database $DATABASE_NAME does not exist. Creating..."
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -c "CREATE DATABASE \"$DATABASE_NAME\";"
fi

echo "Creating tables..."
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -d "$DATABASE_NAME" -U "$DB_USER" -c "
CREATE TABLE IF NOT EXISTS public.\"$METADATA_TABLE\" (
    id SERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_name TEXT NOT NULL,
    entity_id TEXT NOT NULL UNIQUE,
    collection_id TEXT,
    dashboard_id TEXT,
    question_ids TEXT,
    status TEXT,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS public.\"$SOLUTIONS_TABLE\" (
    solution_id TEXT PRIMARY KEY,
    external_id TEXT,
    name TEXT,
    description TEXT,
    duration TEXT,
    categories TEXT,
    program_id TEXT,
    program_name TEXT,
    program_external_id TEXT,
    program_description TEXT,
    private_program BOOLEAN
);

CREATE TABLE IF NOT EXISTS public.\"$PROJECTS_TABLE\" (
    project_id TEXT PRIMARY KEY,
    solution_id TEXT REFERENCES public.\"$SOLUTIONS_TABLE\"(solution_id),
    created_by TEXT,
    created_date TEXT,
    completed_date TEXT,
    last_sync TEXT,
    updated_date TEXT,
    status TEXT,
    remarks TEXT,
    evidence TEXT,
    evidence_count TEXT,
    program_id TEXT,
    task_count TEXT,
    user_role_ids TEXT,
    user_roles TEXT,
    org_id TEXT,
    org_name TEXT,
    org_code TEXT,
    state_id TEXT,
    state_name TEXT,
    district_id TEXT,
    district_name TEXT,
    block_id TEXT,
    block_name TEXT,
    cluster_id TEXT,
    cluster_name TEXT,
    school_id TEXT,
    school_name TEXT,
    certificate_template_id TEXT,
    certificate_template_url TEXT,
    certificate_issued_on TEXT,
    certificate_status TEXT,
    certificate_pdf_path TEXT
);

CREATE TABLE IF NOT EXISTS public.\"$TASKS_TABLE\" (
    task_id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES public.\"$PROJECTS_TABLE\"(project_id),
    name TEXT,
    assigned_to TEXT,
    start_date TEXT,
    end_date TEXT,
    synced_at TEXT,
    is_deleted TEXT,
    is_deletable TEXT,
    remarks TEXT,
    status TEXT,
    evidence TEXT,
    evidence_count TEXT
);

CREATE TABLE IF NOT EXISTS public.\"$REPORT_CONFIG_TABLE\" (
    id SERIAL PRIMARY KEY,
    dashboard_name TEXT NOT NULL,
    report_name TEXT NOT NULL,
    question_type TEXT NOT NULL,
    config JSON NOT NULL
);
"

# Process folders and load data
export PGPASSWORD="$DB_PASSWORD"
for main_folder_name in "$MAIN_FOLDER"/*; do
    if [[ -d "$main_folder_name" ]]; then
        dashboard_name=$(basename "$main_folder_name")

        for folder_name in "$main_folder_name"/*; do
            if [[ -d "$folder_name" ]]; then
                report_name=$(basename "$folder_name")
                json_folder_path="$folder_name/json"
                if [[ ! -d "$json_folder_path" ]]; then
                    echo "No 'json' folder found in $folder_name"
                    continue
                fi
                for query_type_path in "$json_folder_path"/*; do
                    if [[ -d "$query_type_path" ]]; then
                        query_type=$(basename "$query_type_path")
                        for json_file in "$query_type_path"/*.json; do
                            if [[ -f "$json_file" && "$json_file" == *.json ]]; then
                                config=$(cat "$json_file")
                                echo "Inserting into table: $REPORT_CONFIG_TABLE"
                                psql -d "$DATABASE_NAME" -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -c "
                                    INSERT INTO \"$REPORT_CONFIG_TABLE\" (dashboard_name, report_name, question_type, config)
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
unset PGPASSWORD
echo "Tables created and report config data loaded successfully."