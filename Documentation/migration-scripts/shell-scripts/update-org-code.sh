#!/bin/bash

# -------------------------------------------------------------------
# Script : update-org-code.sh
# Purpose: Update the org_code columns in all the tables by converting
#          the organization codes to lowercase and replacing any
#          spaces with underscores.
# -------------------------------------------------------------------

# Source configuration from common-config.env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/common-config.env" ]]; then
  source "$SCRIPT_DIR/common-config.env"
else
  echo "Error: common-config.env not found in $SCRIPT_DIR"
  exit 1
fi

# Set database variables from common-config.env
ENVIRONMENT="${ENVIRONMENT}"
PGHOST="${PGHOST}"
PGPORT="${PGPORT}"
PGDBNAME="${PGDBNAME}"
PGUSER="${PGUSER}"
export PGPASSWORD

LOG_FILE="normalize-org-codes.log"
DRY_RUN=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--dry-run]"
      exit 1
      ;;
  esac
done

log() {
  echo -e "$(date '+%Y-%m-%d %H:%M:%S') $*" | tee -a "$LOG_FILE"
}

MATCHED_TABLES=()

# === Fetch All Table Names ===
log "🔍 Fetching public schema table names..."
table_names=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='public';")

survey_status_ids=()
survey_question_ids=()
observation_domain_ids=()
observation_status_ids=()
observation_question_ids=()

# === Classify tables ===
for tbl in $table_names; do
  if [[ $tbl =~ ^([a-f0-9]{24})_survey_status$ ]]; then
    survey_status_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^([a-f0-9]{24})_questions$ ]]; then
    observation_question_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^([a-f0-9]{24})_status$ ]]; then
    observation_status_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^([a-f0-9]{24})_domain$ ]]; then
    observation_domain_ids+=("${BASH_REMATCH[1]}")
  elif [[ $tbl =~ ^[a-f0-9]{24}$ ]]; then
    survey_question_ids+=("$tbl")
  fi
done

log "🧾 Survey Status Tables: ${#survey_status_ids[@]}"
log "🧾 Survey Questions Tables: ${#survey_question_ids[@]}"
log "🧾 Observation Domain Tables: ${#observation_domain_ids[@]}"
log "🧾 Observation Status Tables: ${#observation_status_ids[@]}"
log "🧾 Observation Questions Tables: ${#observation_question_ids[@]}"

# === Normalize function ===
normalize_table() {
  local table=$1
  local column=$2

  log "🔎 Checking $table.$column for normalization needs..."

  # Count rows needing normalization
  local count=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
    "SELECT COUNT(*) FROM \"${table}\" WHERE ${column} ~ '[A-Z ]';")

  if [[ "$count" =~ ^[0-9]+$ && "$count" -gt 0 ]]; then
    log "⚠️  Found $count rows needing normalization in $table.$column"
    MATCHED_TABLES+=("$table.$column")

    # In dry run mode, show sample rows that will be updated
    if [[ "$DRY_RUN" == true ]]; then
      log "📋 Sample rows from $table.$column that will be updated:"
      psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -tc \
        "SELECT DISTINCT ${column} FROM \"${table}\" WHERE ${column} ~ '[A-Z ]' LIMIT 10;" | while read -r sample; do
        if [[ ! -z "$sample" ]]; then
          normalized=$(echo "$sample" | tr '[:upper:]' '[:lower:]' | sed 's/ /_/g')
          log "   '$sample' → '$normalized'"
        fi
      done
    fi
  else
    log "✔ No normalization needed in $table.$column"
  fi
}

# === Loop through tables ===
for id in "${observation_domain_ids[@]}"; do
  normalize_table "${id}_domain" "org_code"
done

for id in "${observation_status_ids[@]}"; do
  normalize_table "${id}_status" "org_code"
done

for id in "${observation_question_ids[@]}"; do
  normalize_table "${id}_questions" "org_code"
done

for id in "${survey_question_ids[@]}"; do
  normalize_table "${id}" "organisation_code"
done

for id in "${survey_status_ids[@]}"; do
  normalize_table "${id}_survey_status" "organisation_code"
done

normalize_table "${ENVIRONMENT}_projects" "org_code"

log "--------------------------------------------------"
log "Tables requiring normalization:"
printf '  - %s\n' "${MATCHED_TABLES[@]}" | tee -a "$LOG_FILE"

if [[ "$DRY_RUN" == true ]]; then
  log ""
  log "🔍 DRY RUN MODE - No changes will be made to the database"
  log "To execute the actual updates, run: $0"
else
  log ""
  log "💾 PRODUCTION MODE - Changes will be applied to the database"
fi

# === Update function ===
normalize_update() {
  log "🔧 Starting normalization updates..."

  for entry in "${MATCHED_TABLES[@]}"; do
    table="${entry%%.*}"
    column="${entry##*.}"

    update_query="
      UPDATE \"${table}\"
      SET ${column} = LOWER(REGEXP_REPLACE(${column}, ' +', '_', 'g'))
      WHERE ${column} ~ '[A-Z ]';
    "

    if [[ "$DRY_RUN" == true ]]; then
      log "🟡 [DRY RUN] Would execute update on $table.$column"
      log "Query: $update_query"
    else
      log "Executing update on $table.$column"
      psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c "$update_query"

      updated=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
        "SELECT COUNT(*) FROM \"${table}\"
        WHERE ${column} !~ '[A-Z ]'")

      log "✅ Updated $updated rows in $table.$column"
    fi
  done

  if [[ "$DRY_RUN" == true ]]; then
    log "✨ Dry run completed. No database changes were made."
  else
    log "✨ All normalization updates completed."
  fi
}

normalize_update

