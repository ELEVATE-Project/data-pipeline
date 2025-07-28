#!/bin/bash

# === Logging Setup ===
LOG_FILE="project_observation_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

# === PostgreSQL Connection ===
PGHOST="localhost"
PGPORT="5432"
PGDBNAME="test"
PGUSER="postgres"
PROJECT_TABLE_NAME="local_projects"
SOLUTION_TABLE_NAME="local_solutions"
PGPASSWORD="postgres"
export PGPASSWORD

log "🚀 Starting Project Table Alteration Script"

# === Check and Add Columns If Not Exists ===
for column in "program_name TEXT" "tenant_id TEXT"; do
  col_name=$(echo $column | awk '{print $1}')
  log "🔍 Checking column '$col_name' in table '$PROJECT_TABLE_NAME'"

  exists=$(psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDBNAME" -tAc "SELECT 1 FROM information_schema.columns WHERE table_name='$PROJECT_TABLE_NAME' AND column_name='$col_name'")

  if [ "$exists" != "1" ]; then
    log "➕ Adding column '$column' to '$PROJECT_TABLE_NAME'"
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDBNAME" -c "ALTER TABLE $PROJECT_TABLE_NAME ADD COLUMN $column;"
  else
    log "✅ Column '$col_name' already exists"
  fi
done

# === Update program_name from solutions table ===
log "🔁 Updating 'program_name' in '$PROJECT_TABLE_NAME' using '$SOLUTION_TABLE_NAME'"
psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDBNAME" -c \
"UPDATE $PROJECT_TABLE_NAME proj
SET program_name = sol.program_name
FROM $SOLUTION_TABLE_NAME sol
WHERE proj.solution_id = sol.solution_id AND (proj.program_name IS NULL OR proj.program_name = '');"

log "✅ Table alteration and update completed."