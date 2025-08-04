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
PGDBNAME="test"
PGUSER="postgres"
PGPASSWORD="postgres"
export PGPASSWORD

# === Configurable variables ===
AUTH_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7ImlkIjoxLCJuYW1lIjoiTmV2aWwiLCJzZXNzaW9uX2lkIjoxMDgxNCwib3JnYW5pemF0aW9uX2lkcyI6WyIxIl0sIm9yZ2FuaXphdGlvbl9jb2RlcyI6WyJkZWZhdWx0X2NvZGUiXSwidGVuYW50X2NvZGUiOiJkZWZhdWx0Iiwib3JnYW5pemF0aW9ucyI6W3siaWQiOjEsIm5hbWUiOiJEZWZhdWx0IE9yZ2FuaXphdGlvbiIsImNvZGUiOiJkZWZhdWx0X2NvZGUiLCJkZXNjcmlwdGlvbiI6IkRlZmF1bHQgIFNMIE9yZ2FuaXNhdGlvbiIsInN0YXR1cyI6IkFDVElWRSIsInJlbGF0ZWRfb3JncyI6bnVsbCwidGVuYW50X2NvZGUiOiJkZWZhdWx0IiwibWV0YSI6bnVsbCwiY3JlYXRlZF9ieSI6bnVsbCwidXBkYXRlZF9ieSI6bnVsbCwicm9sZXMiOlt7ImlkIjo1LCJ0aXRsZSI6Im1lbnRlZSIsImxhYmVsIjpudWxsLCJ1c2VyX3R5cGUiOjAsInN0YXR1cyI6IkFDVElWRSIsIm9yZ2FuaXphdGlvbl9pZCI6MSwidmlzaWJpbGl0eSI6IlBVQkxJQyIsInRlbmFudF9jb2RlIjoiZGVmYXVsdCIsInRyYW5zbGF0aW9ucyI6bnVsbH0seyJpZCI6NiwidGl0bGUiOiJhZG1pbiIsImxhYmVsIjpudWxsLCJ1c2VyX3R5cGUiOjEsInN0YXR1cyI6IkFDVElWRSIsIm9yZ2FuaXphdGlvbl9pZCI6MSwidmlzaWJpbGl0eSI6IlBVQkxJQyIsInRlbmFudF9jb2RlIjoiZGVmYXVsdCIsInRyYW5zbGF0aW9ucyI6bnVsbH0seyJpZCI6MjUsInRpdGxlIjoibGVhcm5lciIsImxhYmVsIjoiTGVhcm5lciIsInVzZXJfdHlwZSI6MCwic3RhdHVzIjoiQUNUSVZFIiwib3JnYW5pemF0aW9uX2lkIjoxLCJ2aXNpYmlsaXR5IjoiUFVCTElDIiwidGVuYW50X2NvZGUiOiJkZWZhdWx0IiwidHJhbnNsYXRpb25zIjpudWxsfV19XX0sImlhdCI6MTc1NDMwNDIyMiwiZXhwIjoxNzU0MzkwNjIyfQ.nL3YJAnGt6d-2GWmhWOjx0MZomrKiXVA0OTIt21Dn8g"
API_URL="https://saas-qa.tekdinext.com/survey/v1/admin/dbFind/observationSubmissions"

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
ALTER TABLE "${solution_id}_status" ADD COLUMN entity_type TEXT;
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
ALTER TABLE "${solution_id}_status" ADD COLUMN submission_number INTEGER;
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
ALTER TABLE "${solution_id}_domain" ADD COLUMN entity_type TEXT;
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
ALTER TABLE "${solution_id}_questions" ADD COLUMN entity_type TEXT;
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
ALTER TABLE "${solution_id}_questions" ADD COLUMN submission_number INTEGER;
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

# === Alter all Observation Question Tables to include report_type ===
log ""
log ""
log "📌 Altering Observation Question Tables for report_type, entity_id, entity_name, entity_external_id "
for table in "${question_ids[@]}"; do
  log "🔧 Altering table: ${table}_questions"

  if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c "
    ALTER TABLE \"${table}_questions\" ADD COLUMN IF NOT EXISTS report_type TEXT;
    ALTER TABLE \"${table}_questions\" ADD COLUMN IF NOT EXISTS entity_id TEXT;
    ALTER TABLE \"${table}_questions\" ADD COLUMN IF NOT EXISTS entity_name TEXT;
    ALTER TABLE \"${table}_questions\" ADD COLUMN IF NOT EXISTS entity_external_id TEXT;
  "; then
    log "✅ Columns added or already exist in ${table}_questions"

    if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c \
      "UPDATE \"${table}_questions\" SET report_type = 'Default' WHERE report_type IS NULL OR report_type = '';" ; then
      log "✅ Successfully updated rows in ${table}_questions"
    else
      log "❌ Failed to update rows in ${table}_questions"
    fi

  else
    log "❌ Failed to add columns to ${table}_questions"
    continue
  fi
done


# Fetch entity_ids
entity_ids=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -A -c \
"SELECT entity_id FROM local_dashboard_metadata WHERE entity_type = 'solution' AND report_type = 'observation';")

for solution_id in $entity_ids; do
  # Call API
  response=$(curl -s --location "$API_URL" \
    --header "x-auth-token: $AUTH_TOKEN" \
    --header "appname: mentored" \
    --header "Content-Type: application/json" \
    --data '{
      "query": {
        "solutionId":"'"$solution_id"'"
      },
      "sort": {
        "createdAt": "-1"
      },
      "projection": ["entityId", "entityInformation.name", "entityInformation.externalId"],
      "mongoIdKeys": ["solutionId"],
      "Limit": 1
    }')

  # Parse response
  entityId=$(echo "$response" | jq -r '.result[0].entityId // empty')
  entityExternalId=$(echo "$response" | jq -r '.result[0].entityInformation.externalId // empty')
  entityName=$(echo "$response" | jq -r '.result[0].entityInformation.name // empty')

  if [[ -n "$entityId" && -n "$entityExternalId" && -n "$entityName" ]]; then
    for table in "${solution_id}_status" "${solution_id}_domain" "${solution_id}_questions"; do
   # Check if the table exists in the public schema
      table_exists_result=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -c \
        "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = '$table');")
        # The result from psql with -t has leading/trailing whitespace, so we trim it
      if [[ "$(echo "$table_exists_result" | xargs)" == "t" ]]; then
         # Table exists, so we can proceed with the update
        if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c \
          "UPDATE \"$table\" SET entity_id = '$entityId', entity_external_id = '$entityExternalId', entity_name = '$entityName';"
        then
          log "✅ Successfully updated columns in $table"
        else
          log "❌ Failed to update columns in $table"
        fi
      else
        log "❌ Table $table does not exist in the public schema, skipping update."
      fi
    done
    echo "Inserted for $entityId"
  else
    echo "No valid data for solutionId: $solution_id"
  fi
done

log "🏁 All observation tables processed successfully!"
