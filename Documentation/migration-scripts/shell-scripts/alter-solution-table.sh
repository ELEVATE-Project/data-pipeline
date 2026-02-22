#!/usr/bin/env bash
set -euo pipefail

# This script ensures the org_id column exists in the solutions table,
# fetches distinct program_ids, retrieves orgId via API,
# and updates org_id while logging all operations.

# --- Load Configuration ---
CONFIG_FILE="/app/Documentation/migration-scripts/common-config.env"
if [ -f "$CONFIG_FILE" ]; then
  source "$CONFIG_FILE"
else
  echo "❌ Config file not found: $CONFIG_FILE" >&2
  exit 1
fi

# --- Logging setup ---
mkdir -p "$(dirname "$LOG_FILE")"
touch "$LOG_FILE"
log() { local m="$1"; printf '%s - %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$m" | tee -a "$LOG_FILE"; }

trap 'log "Script exited with code $?."' EXIT

PG_CONN="postgresql://$PGUSER:$PGPASSWORD@$PGHOST:$PGPORT/$PGDBNAME"

log "📌 Checking if org_id column exists in ${SOLUTION_TABLE}..."
if ! psql "$PG_CONN" -t -A -c \
  "SELECT 1 FROM information_schema.columns WHERE table_name='${SOLUTION_TABLE}' AND column_name='org_id';" \
  | grep -q 1; then
  log "🛠️ Adding org_id column to ${SOLUTION_TABLE}"
  psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "ALTER TABLE \"${SOLUTION_TABLE}\" ADD COLUMN org_id TEXT;" \
    && log "✅ org_id column added successfully" || { log "❌ Failed to add org_id column"; exit 1; }
else
  log "ℹ️ org_id column already exists"
fi

log "📦 Fetching distinct program_ids from ${SOLUTION_TABLE}..."
# Read results into an array, one program_id per line
mapfile -t program_ids < <(psql "$PG_CONN" -t -A -v ON_ERROR_STOP=1 -c "SELECT DISTINCT program_id FROM ${SOLUTION_TABLE} WHERE program_id IS NOT NULL;")

if [ "${#program_ids[@]}" -eq 0 ]; then
  log "❌ No program_ids found!"
  exit 0
fi

log "✅ Found ${#program_ids[@]} program_ids to process."

for program_id in "${program_ids[@]}"; do
  log "🔍 Processing program_id: $program_id"

  response=$(curl -sS --location "$API_URL" \
    --header "x-auth-token: $AUTH_TOKEN" \
    --header "appname: $APP_NAME" \
    --header "Content-Type: application/json" \
    --data "{\"query\":{\"_id\":\"$program_id\"},\"sort\":{\"createdAt\":\"-1\"},\"mongoIdKeys\":[\"_id\"]}" ) || {
      log "❌ curl failed for program_id: $program_id"
      continue
  }

  orgId=$(echo "$response" | jq -r '.result[0].orgId // empty' || true)

  if [ -z "$orgId" ] || [ "$orgId" = "null" ]; then
    log "⚠️  No orgId found for program_id: $program_id"
    continue
  fi

  # Escape single quotes for SQL literal
  esc_orgId=${orgId//\'/\'\'}
  esc_program_id=${program_id//\'/\'\'}

  # Update using psql; fail on error
  if psql "$PG_CONN" -v ON_ERROR_STOP=1 -c "UPDATE ${SOLUTION_TABLE} SET org_id = '${esc_orgId}' WHERE program_id = '${esc_program_id}';" >/dev/null; then
    log "✅ Updated org_id=$orgId for program_id=$program_id"
  else
    log "❌ Failed to update for program_id=$program_id"
  fi
done

log "🎯 All program_ids processed."
