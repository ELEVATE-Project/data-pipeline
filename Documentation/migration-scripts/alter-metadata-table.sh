#!/bin/bash

# === PostgreSQL connection details ===
PGHOST="localhost"
PGPORT="5432"
PGDBNAME="qa_elevate_data"
PGUSER="postgres"
PGPASSWORD="postgres"
export PGPASSWORD

# === Configurable variables ===
TABLE_NAME="qa_dashboard_metadata"
AUTH_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7ImlkIjoxLCJuYW1lIjoiTmV2aWwiLCJzZXNzaW9uX2lkIjo3Mzg0LCJvcmdhbml6YXRpb25faWRzIjpbIjEiXSwib3JnYW5pemF0aW9uX2NvZGVzIjpbImRlZmF1bHRfY29kZSJdLCJ0ZW5hbnRfY29kZSI6ImRlZmF1bHQiLCJvcmdhbml6YXRpb25zIjpbeyJpZCI6MSwibmFtZSI6IkRlZmF1bHQgT3JnYW5pemF0aW9uIiwiY29kZSI6ImRlZmF1bHRfY29kZSIsImRlc2NyaXB0aW9uIjoiRGVmYXVsdCAgU0wgT3JnYW5pc2F0aW9uIiwic3RhdHVzIjoiQUNUSVZFIiwicmVsYXRlZF9vcmdzIjpudWxsLCJ0ZW5hbnRfY29kZSI6ImRlZmF1bHQiLCJtZXRhIjpudWxsLCJjcmVhdGVkX2J5IjpudWxsLCJ1cGRhdGVkX2J5IjpudWxsLCJyb2xlcyI6W3siaWQiOjUsInRpdGxlIjoibWVudGVlIiwibGFiZWwiOm51bGwsInVzZXJfdHlwZSI6MCwic3RhdHVzIjoiQUNUSVZFIiwib3JnYW5pemF0aW9uX2lkIjoxLCJ2aXNpYmlsaXR5IjoiUFVCTElDIiwidGVuYW50X2NvZGUiOiJkZWZhdWx0IiwidHJhbnNsYXRpb25zIjpudWxsfSx7ImlkIjo2LCJ0aXRsZSI6ImFkbWluIiwibGFiZWwiOm51bGwsInVzZXJfdHlwZSI6MSwic3RhdHVzIjoiQUNUSVZFIiwib3JnYW5pemF0aW9uX2lkIjoxLCJ2aXNpYmlsaXR5IjoiUFVCTElDIiwidGVuYW50X2NvZGUiOiJkZWZhdWx0IiwidHJhbnNsYXRpb25zIjpudWxsfSx7ImlkIjoyNSwidGl0bGUiOiJsZWFybmVyIiwibGFiZWwiOiJMZWFybmVyIiwidXNlcl90eXBlIjowLCJzdGF0dXMiOiJBQ1RJVkUiLCJvcmdhbml6YXRpb25faWQiOjEsInZpc2liaWxpdHkiOiJQVUJMSUMiLCJ0ZW5hbnRfY29kZSI6ImRlZmF1bHQiLCJ0cmFuc2xhdGlvbnMiOm51bGx9XX1dfSwiaWF0IjoxNzUxMDA2MzYwLCJleHAiOjE3NTEwOTI3NjB9.BMQ6jgvkZ3DhOH1ZGLcKX7e89JbGh59tEd5xsJJ0jGI"
SURVEY_API="https://saas-qa.tekdinext.com/survey/v1/admin/dbFind/solutions"
PROJECT_API="https://saas-qa.tekdinext.com/project/v1/admin/dbFind/solutions"

# === Logging ===
LOG_FILE="alter_metadata_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
    echo "$(date +'%Y-%m-%d %H:%M:%S') - $*"
}

log "🚀 Migration started."

# === Step 1: Alter Table if columns missing ===
log "🔧 Checking and altering table columns if needed."
psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -v ON_ERROR_STOP=1 -c "
DO \$\$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '$TABLE_NAME' AND column_name = 'report_type') THEN
    EXECUTE format('ALTER TABLE %I ADD COLUMN report_type TEXT;', '$TABLE_NAME');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '$TABLE_NAME' AND column_name = 'is_rubrics') THEN
    EXECUTE format('ALTER TABLE %I ADD COLUMN is_rubrics BOOLEAN;', '$TABLE_NAME');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '$TABLE_NAME' AND column_name = 'parent_name') THEN
    EXECUTE format('ALTER TABLE %I ADD COLUMN parent_name TEXT;', '$TABLE_NAME');
  END IF;
  IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = '$TABLE_NAME' AND column_name = 'linked_to') THEN
    EXECUTE format('ALTER TABLE %I ADD COLUMN linked_to TEXT;', '$TABLE_NAME');
  END IF;
END
\$\$;
"


# === Step 2: Fetch entity_ids ===
log "📥 Fetching entity_ids where entity_type = 'solution'"
entity_ids=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -A -c \
"SELECT entity_id FROM $TABLE_NAME WHERE entity_type = 'solution';")

entity_count=$(echo "$entity_ids" | grep -c .)
log "🔢 Total solutions found: $entity_count"

if [[ -z "$entity_ids" ]]; then
  log "❌ No entity_ids found. Exiting."
  exit 1
fi

# === Step 3: Make API calls and update database ===
log "🔁 Starting per-entity update from APIs."
for id in $entity_ids; do
  log "🔍 Processing entity_id: $id"

  for API_URL in "$SURVEY_API" "$PROJECT_API"; do
    response=$(curl -s --location "$API_URL" \
      --header "x-auth-token: $AUTH_TOKEN" \
      --header "Content-Type: application/json" \
      --data '{"query":{"_id":"'"$id"'"},"sort":{"createdAt":"-1"},"mongoIdKeys":["_id"],"projection":["programId","isRubricDriven","entityType","type"]}')

    echo "$response"

    status=$(echo "$response" | jq -r '.status // empty')
    has_result=$(echo "$response" | jq -e '.result | length > 0' >/dev/null && echo "yes" || echo "no")

    if [[ "$status" == "200" && "$has_result" == "yes" ]]; then
      is_rubric=$(echo "$response" | jq -e '.result[0] | has("isRubricDriven")' >/dev/null && echo "$response" | jq -r '.result[0].isRubricDriven' || echo "null")
      parent_name=$(echo "$response" | jq -e '.result[0] | has("entityType")' >/dev/null && echo "$response" | jq -r '.result[0].entityType' || echo "null")
      report_type=$(echo "$response" | jq -e '.result[0] | has("type")' >/dev/null && echo "$response" | jq -r '.result[0].type' || echo "null")
      program_id=$(echo "$response" | jq -e '.result[0] | has("programId")' >/dev/null && echo "$response" | jq -r '.result[0].programId' || echo "null")

      log "✅ API call to $API_URL success for entity_id=$id"
      log "✅ API success. is_rubrics=$is_rubric, parent_name=$parent_name, report_type=$report_type, program_id=$program_id"

      update_sql=$(printf "UPDATE %s SET is_rubrics = %s, parent_name = '%s', report_type = '%s', linked_to = '%s' WHERE entity_id = '%s';" \
        "$TABLE_NAME" "$is_rubric" "$parent_name" "$report_type" "$program_id" "$id")

      if psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -c "$update_sql"; then
        log "✅ Update query executed successfully for entity_id=$id"
      else
        log "❌ Update query failed for entity_id=$id"
      fi
      echo "------------------------------------------------------"

      break  # move to next entity_id if this API worked
    else
      log "⚠️ API call to $API_URL failed for entity_id=$id"
      echo "------------------------------------------------------"
    fi
  done

done

log "🎉 Migration completed."
