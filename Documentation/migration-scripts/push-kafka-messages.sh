#!/bin/bash

# === PostgreSQL connection details ===
PGHOST="localhost"
PGPORT="5432"
PGDBNAME="qa_elevate_data"
PGUSER="postgres"
PGPASSWORD="postgres"
export PGPASSWORD

# === Kafka Details ===
TABLE_NAME="qa_dashboard_metadata"
KAFKA_BROKER="kafka:9092"
TOPIC_1="sl-metabase-project-dashboard-qa"
TOPIC_2="sl-metabase-survey-dashboard-qa"
TOPIC_3="sl-metabase-observation-dashboard-qa"

# === Logging Setup ===
LOG_FILE="kafka_push_log_$(date +'%Y%m%d_%H%M%S').log"
exec > >(tee -a "$LOG_FILE") 2>&1
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

log "🚀 Script started to push kafka events"

# === Generate fields ===
today=$(date '+%Y-%m-%dT%H:%M:%S')
random_id="$(uuidgen)_$today"

# === Create initial JSON payload ===
initial_json=$(jq -nc \
  --arg reportType "Project" \
  --arg publishedAt "$today" \
  --arg targetedDistrict "67c82d37bad58c889bc5a5de" \
  --arg admin "1" \
  --arg targetedState "67c82d0c538125889163f197" \
  --arg _id "$random_id" \
  '{
    _id: $_id,
    reportType: $reportType,
    publishedAt: $publishedAt,
    dashboardData: {
      targetedDistrict: $targetedDistrict,
      admin: $admin,
      targetedState: $targetedState
    }
  }')

# === Push initial event using kafka-console-producer.sh ===
echo "$initial_json" | docker exec -i kafka kafka-console-producer --broker-list kafka:9092 --topic "$TOPIC_1"

if [[ $? -eq 0 ]]; then
  log "✅ First event pushed to Kafka: $random_id"
else
  log "❌ Failed to push first event to Kafka"
fi
sleep 180

log "=== Fetch and Process project-linked dashboard ==="
query_result=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
"SELECT linked_to, entity_id
 FROM $TABLE_NAME
 WHERE entity_type = 'solution'
   AND report_type = 'improvementProject'
   AND linked_to IS NOT NULL AND linked_to <> 'null'
   AND entity_id IS NOT NULL AND entity_id <> 'null';")

log "PROJECT QUERY RESULT = $query_result"

# === Read each row and push to Kafka ===
echo "$query_result" | while IFS='|' read -r linked_to entity_id; do
  today=$(date '+%Y-%m-%dT%H:%M:%S')
  random_id="$(uuidgen)_$today"

  projectJson=$(jq -nc \
    --arg reportType "Project" \
    --arg publishedAt "$today" \
    --arg targetedProgram "$linked_to" \
    --arg targetedSolution "$entity_id" \
    --arg _id "$random_id" \
    '{
      _id: $_id,
      reportType: $reportType,
      publishedAt: $publishedAt,
      dashboardData: {
        targetedProgram: $targetedProgram,
        targetedSolution: $targetedSolution
      }
    }')

  echo "$projectJson" | docker exec -i kafka kafka-console-producer --broker-list kafka:9092 --topic "$TOPIC_1"

  if [[ $? -eq 0 ]]; then
    log "✅ Project event pushed to Kafka: $random_id"
    log "🚀 Event pushed for solution: $entity_id -> program: $linked_to"
  else
    log "❌ Failed to push Project event to Kafka"
  fi
  sleep 90
done
log "=== Completed Processing project-linked dashboard ==="
log ""


log "=== Fetch and Process survey-linked dashboard ==="
query_result=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
"SELECT linked_to, entity_id
 FROM $TABLE_NAME
 WHERE entity_type = 'solution'
   AND report_type = 'survey'
   AND linked_to IS NOT NULL AND linked_to <> 'null'
   AND entity_id IS NOT NULL AND entity_id <> 'null';")

log "SURVEY QUERY RESULT = $query_result"

# === Read each row and push to Kafka ===
echo "$query_result" | while IFS='|' read -r linked_to entity_id; do
  today=$(date '+%Y-%m-%dT%H:%M:%S')
  random_id="$(uuidgen)_$today"

  surveyJson=$(jq -nc \
    --arg reportType "Survey" \
    --arg publishedAt "$today" \
    --arg targetedProgram "$linked_to" \
    --arg targetedSolution "$entity_id" \
    --arg _id "$random_id" \
    '{
      _id: $_id,
      reportType: $reportType,
      publishedAt: $publishedAt,
      dashboardData: {
        targetedProgram: $targetedProgram,
        targetedSolution: $targetedSolution
      }
    }')

  echo "$surveyJson" | docker exec -i kafka kafka-console-producer --broker-list kafka:9092 --topic "$TOPIC_2"

  if [[ $? -eq 0 ]]; then
    log "✅ Survey event pushed to Kafka: $random_id"
    log "🚀 Event pushed for solution: $entity_id -> program: $linked_to"
  else
    log "❌ Failed to push Survey event to Kafka"
  fi
  sleep 90
done
log "=== Completed Processing survey-linked dashboard ==="
log ""



log "=== Fetch and Process observation-linked dashboard ==="
query_result=$(psql -h "$PGHOST" -p "$PGPORT" -d "$PGDBNAME" -U "$PGUSER" -Atc \
"SELECT linked_to, entity_id, is_rubrics, parent_name
 FROM $TABLE_NAME
 WHERE entity_type = 'solution'
   AND report_type = 'observation'
   AND linked_to IS NOT NULL AND linked_to <> 'null'
   AND entity_id IS NOT NULL AND entity_id <> 'null'
   AND parent_name IS NOT NULL AND parent_name <> 'null';")

log "OBSERVATION QUERY RESULT = $query_result"

# === Read each row and push to Kafka ===
echo "$query_result" | while IFS='|' read -r linked_to entity_id is_rubrics parent_name; do
  today=$(date '+%Y-%m-%dT%H:%M:%S')
  random_id="$(uuidgen)_$today"

  observationJson=$(jq -nc \
    --arg reportType "Observation" \
    --arg publishedAt "$today" \
    --arg targetedProgram "$linked_to" \
    --arg targetedSolution "$entity_id" \
    --arg isRubric "$( [[ "$is_rubrics" == "t" ]] && echo true || echo false )"\
    --arg entityType "$parent_name" \
    --arg _id "$random_id" \
    '{
      _id: $_id,
      reportType: $reportType,
      publishedAt: $publishedAt,
      dashboardData: {
        targetedProgram: $targetedProgram,
        targetedSolution: $targetedSolution,
        isRubric: $isRubric,
        entityType:  $entityType
      }
    }')

  echo "$observationJson" | docker exec -i kafka kafka-console-producer --broker-list kafka:9092 --topic "$TOPIC_3"

  if [[ $? -eq 0 ]]; then
    log "✅ Observation event pushed to Kafka: $random_id"
    log "🚀 Event pushed for solution: $entity_id -> program: $linked_to"
  else
    log "❌ Failed to push Observation event to Kafka"
  fi
  sleep 90
done
log "=== Completed Processing observation-linked dashboard ==="
log ""

log "🏁 Script completed"
