#!/bin/bash

# === Logging Setup ===
LOG_FILE="alter_observation_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

# === PostgreSQL Connection ===
PGHOST="localhost"
PGPORT="5432"
PGDBNAME="qa_elevate_data"
PGUSER="postgres"
PGPASSWORD="postgres"
export PGPASSWORD

log "🚀 Starting Observation Table Alteration Script"

# === Fetch All Table Names ===
log "🔍 Fetching public schema table names..."
table_names=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
"SELECT table_name FROM information_schema.tables WHERE table_schema='public';")

status_ids=()
domain_ids=()
question_ids=()

for tbl in $table_names; do
  if [[ $tbl == *_survey_* ]]; then
    continue
  fi
  if [[ $tbl =~ ^(.+)_status$ ]]; then
    status_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^(.+)_domain$ ]]; then
    domain_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^(.+)_questions$ ]]; then
    question_ids+=("${BASH_REMATCH[1]}")
  fi
done

log "🧾 Total Status Tables: ${#status_ids[@]}"
log "🧾 Total Domain Tables: ${#domain_ids[@]}"
log "🧾 Total Question Tables: ${#question_ids[@]}"

# === Alter Status Tables ===
log "📌 Altering Observation Status Tables..."
for solution_id in "${status_ids[@]}"; do
  log "🔧 Altering ${solution_id}_status table..."
  if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" <<EOF
ALTER TABLE "${solution_id}_status" ADD COLUMN entityType TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_one_name TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_one_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_two_name TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_two_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_three_name TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_three_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_four_name TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_four_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_five_name TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN parent_five_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN user_one_profile_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN user_two_profile_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN user_three_profile_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN user_four_profile_id TEXT;
ALTER TABLE "${solution_id}_status" ADD COLUMN submission_number TEXT;
ALTER TABLE "${solution_id}_status" RENAME COLUMN school_id TO user_five_profile_id;
ALTER TABLE "${solution_id}_status" RENAME COLUMN state_name TO user_one_profile_name;
ALTER TABLE "${solution_id}_status" RENAME COLUMN district_name TO user_two_profile_name;
ALTER TABLE "${solution_id}_status" RENAME COLUMN block_name TO user_three_profile_name;
ALTER TABLE "${solution_id}_status" RENAME COLUMN cluster_name TO user_four_profile_name;
ALTER TABLE "${solution_id}_status" RENAME COLUMN school_name TO user_five_profile_name;
EOF
  then
    log "✅ Successfully altered ${solution_id}_status table"
  else
    log "❌ Failed to alter ${solution_id}_status table"
  fi
done

# === Alter Domain Tables ===
log "📌 Altering Observation Domain Tables..."
for solution_id in "${domain_ids[@]}"; do
  log "🔧 Altering ${solution_id}_domain table..."
  if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" <<EOF
ALTER TABLE "${solution_id}_domain" ADD COLUMN entityType TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_one_name TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_one_id TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_two_name TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_two_id TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_three_name TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_three_id TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_four_name TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_four_id TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_five_name TEXT;
ALTER TABLE "${solution_id}_domain" ADD COLUMN parent_five_id TEXT;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN state_name TO user_one_profile_name;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN state_id TO user_one_profile_id;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN district_name TO user_two_profile_name;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN district_id TO user_two_profile_id;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN block_name TO user_three_profile_name;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN block_id TO user_three_profile_id;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN cluster_name TO user_four_profile_name;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN cluster_id TO user_four_profile_id;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN school_name TO user_five_profile_name;
ALTER TABLE "${solution_id}_domain" RENAME COLUMN school_id TO user_five_profile_id;
EOF
  then
    log "✅ Successfully altered ${solution_id}_domain table"
  else
    log "❌ Failed to alter ${solution_id}_domain table"
  fi
done

# === Alter Question Tables ===
log "📌 Altering Observation Question Tables..."
for solution_id in "${question_ids[@]}"; do
  log "🔧 Altering ${solution_id}_questions table..."
  if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" <<EOF
ALTER TABLE "${solution_id}_questions" ADD COLUMN entityType TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_one_name TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_one_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_two_name TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_two_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_three_name TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_three_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_four_name TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_four_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_five_name TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN parent_five_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN user_one_profile_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN user_two_profile_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN user_three_profile_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN user_four_profile_id TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN submission_number TEXT;
ALTER TABLE "${solution_id}_questions" ADD COLUMN status_of_submission TEXT;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN state_name TO user_one_profile_name;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN district_name TO user_two_profile_name;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN block_name TO user_three_profile_name;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN cluster_name TO user_four_profile_name;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN school_name TO user_five_profile_name;
ALTER TABLE "${solution_id}_questions" RENAME COLUMN school_id TO user_five_profile_id;
EOF
  then
    log "✅ Successfully altered ${solution_id}_questions table"
  else
    log "❌ Failed to alter ${solution_id}_questions table"
  fi
done

log "🏁 All observation tables processed successfully!"
