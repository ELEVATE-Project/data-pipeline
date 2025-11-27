#!/bin/bash

# Export PostgreSQL password
export PGPASSWORD='elevatedata'

DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="elevate_data"

log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') | $1"
}

# Step 1: Disconnect all sessions from elevate_data
log "Terminating all active sessions on $DB_NAME..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d postgres -c "
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = '$DB_NAME' AND pid <> pg_backend_pid();
"
log "✅  All active sessions terminated."

# Step 2: List all non-prod tables and drop them
log "Listing all tables in $DB_NAME database not starting with 'prod'..."

TABLES=$(psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -t -c "
SELECT tablename
FROM pg_tables
WHERE schemaname = 'public' AND tablename NOT LIKE 'prod%';
")

echo "$TABLES"

if [[ -z "$TABLES" ]]; then
    log "✅ No non-prod tables found."
else
    log "Dropping non-prod tables..."
    for tbl in $TABLES; do
        log "  Dropping table: $tbl"
        psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "DROP TABLE IF EXISTS \"$tbl\" CASCADE;"
    done
    log "✅ All non-prod tables dropped."
fi

# Step 3: Truncate data from bellow tables
TABLES_TO_TRUNCATE=("prod_solutions" "prod_projects" "prod_tasks" "prod_dashboard_metadata")

log "Starting truncation of selected tables in $DB_NAME..."

for table in "${TABLES_TO_TRUNCATE[@]}"; do
    log "  Truncating table: $table"
    psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "TRUNCATE TABLE \"$table\" CASCADE;"
done

log "✅ All specified tables truncated successfully."