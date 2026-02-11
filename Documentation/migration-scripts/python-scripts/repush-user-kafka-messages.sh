#!/bin/bash

# === Logging Setup ===
LOG_FILE="re_push_user_kafka_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

# === PostgreSQL connection details ===
PGHOST="PGHOST"
PGPORT="PGPORT"
PGDBNAME="PGDBNAME"
PGUSER="PGUSER"
PGPASSWORD="PGPASSWORD"
export PGPASSWORD
USERS_TABLE="users"

# === API connection details ===
ENTITY_API="ENTITY_API"
INTERNAL_TOKEN="INTERNAL_TOKEN"

# === Kafka connection details ===
KAFKA_BROKER="kafka:9092"
TOPIC="dev.userCreate"

# === Metadata Stats ===
log "📊 Fetching user metadata from '$USERS_TABLE'..."

TOTAL_ROWS=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -c "SELECT COUNT(*) FROM $USERS_TABLE;" | xargs)
ACTIVE_USERS=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -c "SELECT COUNT(*) FROM $USERS_TABLE WHERE status = 'ACTIVE';" | xargs)
DELETED_USERS=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -c "SELECT COUNT(*) FROM $USERS_TABLE WHERE deleted_at IS NOT NULL;" | xargs)

log "------------------------------------"
log "Total Users       : $TOTAL_ROWS"
log "Active Users      : $ACTIVE_USERS"
log "Deleted Users     : $DELETED_USERS"
log "------------------------------------"

# === Fetch and Print Enriched User Info ===
log "📦 Generating enriched user JSONs:"
log "------------------------------------"

psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -F $'\t' -A -c \
"SELECT id, name, username, tenant_code, created_at, updated_at, status, meta FROM $USERS_TABLE ORDER BY created_at;" | \
tail -n +2 | \
while IFS=$'\t' read -r id name username tenant created updated status meta_json; do

  echo
  echo
  echo

  log "🔎 Processing User ID: $id | Name: $name"

  ## ORG LOGIC (merged orgs + roles query)
  merged_orgs=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -t -A -F $'\t' -c "
    SELECT json_build_object(
      'created_by', COALESCE(inv.created_by, uo.user_id),
      'organizations', (
        SELECT json_agg(org_with_roles)
        FROM (
          SELECT
            o.id, o.name, o.code, o.description, o.status,
            o.related_orgs, o.tenant_code, o.meta, o.created_by, o.updated_by,
            (
              SELECT json_agg(ur)
              FROM (
                SELECT
                  ur.id, ur.title, ur.label, ur.user_type, ur.status,
                  ur.organization_id, ur.visibility, ur.tenant_code, ur.translations
                FROM user_organization_roles uor
                JOIN user_roles ur
                  ON uor.role_id = ur.id AND uor.tenant_code = ur.tenant_code
                WHERE uor.user_id = uo.user_id
                  AND uor.organization_code = o.code
                  AND uor.tenant_code = o.tenant_code
                  AND uor.deleted_at IS NULL
              ) ur
            ) AS roles
          FROM user_organizations uo
          JOIN organizations o
            ON o.code = uo.organization_code AND o.tenant_code = uo.tenant_code
          WHERE uo.user_id = $id
        ) org_with_roles
      )
    )
    FROM user_organizations uo
    LEFT JOIN organization_user_invites oui ON oui.username = '$username'
    LEFT JOIN invitations inv ON inv.id = oui.invitation_id
    WHERE uo.user_id = $id
    LIMIT 1;
  ")

  [ -z "$id" ] && continue

  deleted="false"
  if [[ "$status" == "DELETED" ]]; then deleted="true"; fi

  esc_name=$(printf '%s' "$name" | sed 's/"/\\"/g')
  esc_username=$(printf '%s' "$username" | sed 's/"/\\"/g')

  if ! echo "$meta_json" | jq empty 2>/dev/null; then
    log "⚠️  [User ID: $id] Invalid JSON in meta field — Skipping"
    continue
  fi

  is_empty=$(echo "$meta_json" | jq 'length == 0')
  if [ "$is_empty" == "true" ]; then
    log "⚠️  [User ID: $id] Empty meta object — Skipping"
    continue
  fi

  entity_ids=$(echo "$meta_json" | jq -r '
    try to_entries | map(
      if .value | type == "array"
      then .value[]
      else .value
      end
    ) | unique | join(",")' 2>/dev/null)

  if [ -z "$entity_ids" ]; then
    log "⚠️  [User ID: $id] No entity IDs found in meta — Skipping"
    continue
  fi

  quoted_ids=$(echo "$entity_ids" | awk -v RS= -v ORS= '{gsub(/,/, "\",\""); print "[\"" $0 "\"]"}')

  log "📡 [User ID: $id] Fetching entity metadata for: $entity_ids"

  payload=$(jq -n --argjson ids "$quoted_ids" --arg tenant "$tenant" '{
    query: {
      _id: { "$in": $ids },
      tenantId: $tenant
    },
    mongoIdKeys: "_id",
    projection: ["_id", "metaInformation.name", "metaInformation.externalId"]
  }')

  response=$(curl -s -w "HTTPSTATUS:%{http_code}" --location "$ENTITY_API" \
    --header 'content-type: application/json' \
    --header "internal-access-token: $INTERNAL_TOKEN" \
    --data "$payload")

  body=$(echo "$response" | sed -e 's/HTTPSTATUS\:.*//g')
  status_code=$(echo "$response" | tr -d '\n' | sed -e 's/.*HTTPSTATUS://')

  if [[ "$status_code" != "200" ]]; then
    log "🚫 [User ID: $id] API response not 200 — Skipping"
    continue
  fi

  if echo "$body" | grep -q "ENTITY_NOT_FOUND"; then
    log "🚫 [User ID: $id] ENTITY_NOT_FOUND in API — Skipping"
    continue
  fi

  enriched_fields=$(echo "$body" | jq '
    try .result | map({ (.["_id"]): {
      id: ._id,
      name: .metaInformation.name,
      externalId: .metaInformation.externalId
    }}) | add' 2>/dev/null)

  if [ -z "$enriched_fields" ] || [ "$enriched_fields" == "null" ]; then
    log "⚠️  [User ID: $id] Failed to parse entity metadata — Skipping"
    continue
  fi

  final_json=$(jq -n \
    --argjson meta "$meta_json" \
    --argjson enriched "$enriched_fields" '
    [
      "block", "cluster", "district", "school", "state", "professional_role", "professional_subroles"
    ] as $expected |
    reduce $expected[] as $key (
      {}; .[$key] =
        if ($meta[$key] | type == "array")
        then ($meta[$key] | map($enriched[.] // null))
        elif ($meta[$key] != null)
        then $enriched[$meta[$key]]
        else null
        end
    )'
  )

  if [ -z "$final_json" ] || [ "$final_json" == "null" ]; then
    log "⚠️  [User ID: $id] Failed to construct enriched meta — Skipping"
    continue
  fi

  log "✅  [User ID: $id] Outputting enriched user JSON"

  created_by=$(echo "$merged_orgs" | jq -r '.created_by')
  orgs_json=$(echo "$merged_orgs" | jq -c '.organizations')

  user_json=$(jq -c -n --argjson final "$final_json" \
    --argjson organizations "$orgs_json" \
    --arg created_by "$created_by" \
    --arg id "$id" --arg name "$esc_name" --arg username "$esc_username" \
    --arg tenant "$tenant" --arg created "$created" --arg updated "$updated" \
    --arg status "$status" --argjson deleted "$deleted" '
    {
      entity: "user",
      eventType: "create",
      entityId: ($id | tonumber),
      name: $name,
      username: $username,
      tenant_code: $tenant,
      created_at: $created,
      updated_at: $updated,
      status: $status,
      deleted: $deleted,
      id: ($id | tonumber),
      created_by: ($created_by | tonumber),
      organizations: $organizations
    } + $final')

  echo "$user_json"
  echo "$user_json" | docker exec -i kafka kafka-console-producer --broker-list "$KAFKA_BROKER" --topic "$TOPIC"
  log "📤  [User ID: $id] JSON pushed to Kafka topic '$TOPIC'"

done