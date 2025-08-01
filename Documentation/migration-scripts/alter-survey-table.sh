#!/bin/bash

# === Logging Setup ===
LOG_FILE="alter_survey_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

# === PostgreSQL Connection ===
PGHOST="localhost"
PGPORT="5432"
PGDBNAME="test"
PGUSER="postgres"
PGPASSWORD="postgres"
export PGPASSWORD

log "🚀 Starting Survey Table Alteration Script"

# === Fetch All Table Names ===
log "🔍 Fetching public schema table names..."
table_names=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
"SELECT table_name FROM information_schema.tables WHERE table_schema='public';")

# === Prepare survey_status and base table lists ===
survey_status_ids=()
survey_question_ids=()

for tbl in $table_names; do
  if [[ $tbl =~ ^([a-f0-9]{24})_survey_status$ ]]; then
    survey_status_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^[a-f0-9]{24}$ ]]; then
    survey_question_ids+=("$tbl")
  fi
done

log "🧾 Survey Status Tables Found: ${#survey_status_ids[@]}"
printf '  - %s_survey_status\n' "${survey_status_ids[@]}"

log "🧾 Survey Question Tables Found: ${#survey_question_ids[@]}"
printf '  - %s\n' "${survey_question_ids[@]}"

# === Alter all survey question tables ===
log "📌 Altering Survey Question Tables..."
for table in "${survey_question_ids[@]}"; do
  log "🔧 Altering table: $table"
  # Add the column if not exists
  if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c \
    "ALTER TABLE \"$table\" ADD COLUMN IF NOT EXISTS report_type TEXT;"; then
    log "✅ Column added or already exists in $table"

    # Update rows only where report_type is NULL or empty
    if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c \
      "UPDATE \"$table\" SET report_type = 'Default' WHERE report_type IS NULL OR report_type = '';" ; then
      log "✅ Successfully updated rows in $table"
    else
      log "❌ Failed to update rows in $table"
    fi

  else
    log "❌ Failed to add column to $table"
    continue
  fi
done

log "🏁 All valid survey tables processed!"
