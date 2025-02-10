#!/bin/bash

# Load environment variables from .env file
set -a
source ./config.env
set +a

# Set dynamic table names
METADATA_TABLE="${PG_ENV}_dashboard_metadata"
SOLUTIONS_TABLE="${PG_ENV}_solutions"
PROJECTS_TABLE="${PG_ENV}_projects"
TASKS_TABLE="${PG_ENV}_tasks"

# Check if required variables are set
if [[ -z "$PG_HOST" || -z "$PG_PORT" || -z "$PG_DBNAME" || -z "$POSTGRES_USER" || -z "$POSTGRES_PASSWORD" ]]; then
    echo "Error: One or more required PostgreSQL environment variables are missing."
    exit 1
fi

# SQL script for table creation
SQL_COMMANDS=$(cat <<EOF
CREATE TABLE IF NOT EXISTS public."$METADATA_TABLE"
(
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

CREATE TABLE IF NOT EXISTS public."$SOLUTIONS_TABLE"
(
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

CREATE TABLE IF NOT EXISTS public."$PROJECTS_TABLE"
(
    project_id TEXT PRIMARY KEY,
    solution_id TEXT REFERENCES public."$SOLUTIONS_TABLE"(solution_id),
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

CREATE TABLE IF NOT EXISTS public."$TASKS_TABLE"
(
    task_id TEXT PRIMARY KEY,
    project_id TEXT REFERENCES public."$PROJECTS_TABLE"(project_id),
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
EOF
)

# Execute SQL commands
PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$PG_HOST" -p "$PG_PORT" -d "$PG_DBNAME" -U "$POSTGRES_USER" -c "$SQL_COMMANDS"

if [ $? -eq 0 ]; then
    echo "Tables created successfully."
else
    echo "Error: Failed to create tables."
fi
