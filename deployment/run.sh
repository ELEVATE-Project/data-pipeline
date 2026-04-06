#!/bin/bash

ENVIRONMENT=$1  # Input: dev or qa environment
JOB_NAME="${ENVIRONMENT}_"
FLINK_SERVER_URL="http://localhost:8081"
FLINK_DIR="/opt/flink-1.13.2"
CODE_BASE_PATH=$(dirname "$(pwd)")
DEV_REPLACE_DIR="/opt/work-shop/dev/resource/replacementFile/"
QA_REPLACE_DIR="/opt/work-shop/qa/resource/replacementFile/"

# Define color codes for better visibility
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Input validation for environment
if [[ "$ENVIRONMENT" != "dev" && "$ENVIRONMENT" != "qa" ]]; then
  echo -e "${RED}Error:${NC} Invalid environment specified. Must be 'dev' or 'qa'. Please provide a valid environment."
  exit 1
fi

echo -e "${CYAN}Starting deployment process for environment: ${MAGENTA}$ENVIRONMENT${NC}"
echo -e "${YELLOW}CODE_BASE_PATH: ${MAGENTA}$CODE_BASE_PATH${NC}"

# Fetch and display currently running jobs in Flink
echo -e "\n${CYAN}Fetching currently running jobs from Flink...${NC}"
RUNNING_JOBS=$(curl -s "$FLINK_SERVER_URL/jobs/overview" | jq -r '.jobs[] | [.name, .jid, .state] | @tsv')

if [[ -n "$RUNNING_JOBS" ]]; then
  echo -e "\n${CYAN}Currently running jobs in Flink:${NC}"
  echo -e "${YELLOW}NAME\t\t\t\tID\t\t\t\tSTATUS${NC}"
  echo "$RUNNING_JOBS" | while IFS=$'\t' read -r JOB_NAME JOB_ID STATUS; do
    # Color status based on its value
    if [[ "$STATUS" == "RUNNING" ]]; then
      STATUS_COLOR="${GREEN}$STATUS${NC}"
    elif [[ "$STATUS" == "FAILED" ]]; then
      STATUS_COLOR="${RED}$STATUS${NC}"
    else
      STATUS_COLOR="${CYAN}$STATUS${NC}"
    fi
    echo -e "$JOB_NAME\t$JOB_ID\t$STATUS_COLOR"
  done
else
  echo -e "${RED}No jobs are currently running.${NC}"
fi

# Fetch detailed running job data in JSON format and Extract Job IDs based on JOB_NAME prefix
RUNNING_JOB_JSON=$(curl -s "$FLINK_SERVER_URL/jobs/overview")
JOB_IDS=$(echo "$RUNNING_JOB_JSON" | jq -r --arg job_name "$JOB_NAME" '.jobs[] | select(.name | test("^" + $job_name; "i")) | select(.state == "RUNNING") | .jid')

# Check and cancel running jobs
if [[ -n "$JOB_IDS" ]]; then
  echo -e "\n${CYAN}Found running jobs with the prefix '$JOB_NAME'. Proceeding with cancellation...${NC}"
  for JOB_ID in $JOB_IDS; do
    echo -e "${CYAN}Attempting to cancel job with ID: $JOB_ID${NC}"
    OUTPUT=$("$FLINK_DIR/bin/flink" cancel "$JOB_ID" -m "localhost:8081")

    if echo "$OUTPUT" | grep -q "Cancelled job $JOB_ID"; then
      echo -e "${GREEN}Success:${NC} Job $JOB_ID successfully canceled."
    else
      echo -e "${RED}Error:${NC} Failed to cancel job $JOB_ID. Output: $OUTPUT"
    fi
  done
else
  echo -e "${RED}No running jobs found with the prefix '${MAGENTA}$JOB_NAME${RED}' in the ${MAGENTA}$ENVIRONMENT${RED} environment.${NC}"
  echo ""
fi

# File paths to be replaced
FILES_TO_REPLACE=(
  "$CODE_BASE_PATH/jobs-core/src/main/resources/base-config.conf"
  "$CODE_BASE_PATH/metabase-jobs/users-via-csv/src/main/resources/application.conf"
  "$CODE_BASE_PATH/metabase-jobs/observation-dashboard-creator/src/main/resources/metabase-observation-dashboard.conf"
  "$CODE_BASE_PATH/metabase-jobs/project-dashboard-creator/src/main/resources/metabase-project-dashboard.conf"
  "$CODE_BASE_PATH/metabase-jobs/survey-dashboard-creator/src/main/resources/metabase-survey-dashboard.conf"
  "$CODE_BASE_PATH/stream-jobs/observation-stream-processor/src/main/resources/observation-stream.conf"
  "$CODE_BASE_PATH/stream-jobs/project-stream-processor/src/main/resources/project-stream.conf"
  "$CODE_BASE_PATH/stream-jobs/survey-stream-processor/src/main/resources/survey-stream.conf"
  "$CODE_BASE_PATH/stream-jobs/user-stream-processor/src/main/resources/user-stream.config"
  "$CODE_BASE_PATH/stream-jobs/mentoring-stream-processor/src/main/resources/mentoring-stream.conf"
  "$CODE_BASE_PATH/metabase-jobs/user-service/src/main/resources/user-service.conf"
  "$CODE_BASE_PATH/metabase-jobs/user-dashboard-creator/src/main/resources/metabase-user-dashboard.conf"
  "$CODE_BASE_PATH/metabase-jobs/mentoring-dashboard-creator/src/main/resources/metabase-mentoring-dashboard.conf"
)

# Set the correct source directory based on the environment
if [[ "$ENVIRONMENT" == "dev" ]]; then
  SOURCE_DIR="$DEV_REPLACE_DIR"
elif [[ "$ENVIRONMENT" == "qa" ]]; then
  SOURCE_DIR="$QA_REPLACE_DIR"
else
  echo -e "${RED}Error:${NC} Invalid environment specified. Must be 'dev' or 'qa'. Please provide a valid environment."
  exit 1
fi

# Replace file content based on environment
echo -e "${CYAN}Replacing file content with files from $SOURCE_DIR...${NC}"
for FILE in "${FILES_TO_REPLACE[@]}"; do
  BASENAME=$(basename "$FILE")
  SOURCE_FILE="$SOURCE_DIR$BASENAME"

  if [[ -f "$SOURCE_FILE" && -f "$FILE" ]]; then
    echo -e "${YELLOW}Replacing content of ${MAGENTA}$FILE${YELLOW} with ${MAGENTA}$SOURCE_FILE${NC}"
    cp "$SOURCE_FILE" "$FILE"
    if [[ $? -eq 0 ]]; then
      echo -e "${GREEN}Success:${NC} Replaced $FILE with $SOURCE_FILE"
    else
      echo -e "${RED}Error:${NC} Failed to replace $FILE"
    fi
  else
    echo -e "${RED}Error:${NC} Missing file: ${MAGENTA}$SOURCE_FILE${NC} or ${MAGENTA}$FILE${NC}"
  fi
done

# Scala file replacement logic
SCALA_JOB_FILES=(
  "$CODE_BASE_PATH/metabase-jobs/observation-dashboard-creator/src/main/scala/org/shikshalokam/job/observation/dashboard/creator/task/ObservationMetabaseDashboardConfig.scala"
  "$CODE_BASE_PATH/metabase-jobs/project-dashboard-creator/src/main/scala/org/shikshalokam/job/dashboard/creator/task/ProjectMetabaseDashboardConfig.scala"
  "$CODE_BASE_PATH/metabase-jobs/survey-dashboard-creator/src/main/scala/org/shikshalokam/job/survey/dashboard/creator/task/SurveyMetabaseDashboardConfig.scala"
  "$CODE_BASE_PATH/stream-jobs/observation-stream-processor/src/main/scala/org/shikshalokam/job/observation/stream/processor/task/ObservationStreamConfig.scala"
  "$CODE_BASE_PATH/stream-jobs/project-stream-processor/src/main/scala/org/shikshalokam/job/project/stream/processor/task/ProjectStreamConfig.scala"
  "$CODE_BASE_PATH/stream-jobs/survey-stream-processor/src/main/scala/org/shikshalokam/job/survey/stream/processor/task/SurveyStreamConfig.scala"
  "$CODE_BASE_PATH/stream-jobs/user-stream-processor/src/main/scala/org/shikshalokam/job/user/stream/processor/task/UserStreamConfig.scala"
  "$CODE_BASE_PATH/stream-jobs/mentoring-stream-processor/src/main/scala/org/shikshalokam/job/mentoring/stream/processor/task/MentoringStreamConfig.scala"
  "$CODE_BASE_PATH/metabase-jobs/user-service/src/main/scala/org/shikshalokam/job/user/service/task/UserServiceConfig.scala"
  "$CODE_BASE_PATH/metabase-jobs/user-dashboard-creator/src/main/scala/org/shikshalokam/job/user/dashboard/creator/task/UserMetabaseDashboardConfig.scala"
  "$CODE_BASE_PATH/metabase-jobs/mentoring-dashboard-creator/src/main/scala/org/shikshalokam/job/mentoring/dashboard/creator/task/MentoringMetabaseDashboardConfig.scala"
)

echo -e "\n${CYAN}Updating job names in Scala files...${NC}"
for SCALA_FILE in "${SCALA_JOB_FILES[@]}"; do
  if [[ -f "$SCALA_FILE" ]]; then
    echo -e "${YELLOW}Processing ${MAGENTA}$SCALA_FILE${NC}"

    # Replace BaseJobConfig line with ENVIRONMENT-prefixed job name
    if grep -q 'BaseJobConfig(config, "' "$SCALA_FILE"; then
      perl -pi -e "s/BaseJobConfig\(config, \"([^\"]+)\"/BaseJobConfig(config, \"${ENVIRONMENT}_\1\"/g" "$SCALA_FILE"
      echo -e "${GREEN}Success:${NC} Replaced job name in $SCALA_FILE with ENVIRONMENT-prefixed value"
    else
      echo -e "${YELLOW}Note:${NC} No matching BaseJobConfig line found in ${MAGENTA}$SCALA_FILE${NC}"
    fi
  else
    echo -e "${RED}Error:${NC} Scala file ${MAGENTA}$SCALA_FILE${RED} does not exist.${NC}"
  fi
done

# Build Maven project with memory limit
echo ""
MAVEN_BUILD_CMD="mvn clean install -DskipTests"
export MAVEN_OPTS="-Xmx1g"  # Limit memory usage to 1GB
echo -e "${CYAN}Building Maven project with limited memory...${NC}"
cd "$CODE_BASE_PATH"
$MAVEN_BUILD_CMD
if [[ $? -eq 0 ]]; then
  echo -e "${GREEN}Success:${NC} Maven build completed successfully"
else
  echo -e "${RED}Error:${NC} Maven build failed"
  exit 1
fi

# Define log directory
LOG_DIR="/opt/work-shop/$ENVIRONMENT/logs"

# Define job JARs to submit
JAR_PATHS=(
  "$CODE_BASE_PATH/metabase-jobs/observation-dashboard-creator/target/observation-dashboard-creator-1.0.0.jar"
  "$CODE_BASE_PATH/metabase-jobs/project-dashboard-creator/target/project-dashboard-creator-1.0.0.jar"
  "$CODE_BASE_PATH/metabase-jobs/survey-dashboard-creator/target/survey-dashboard-creator-1.0.0.jar"
  "$CODE_BASE_PATH/stream-jobs/observation-stream-processor/target/observation-stream-processor-1.0.0.jar"
  "$CODE_BASE_PATH/stream-jobs/project-stream-processor/target/project-stream-processor-1.0.0.jar"
  "$CODE_BASE_PATH/stream-jobs/survey-stream-processor/target/survey-stream-processor-1.0.0.jar"
  "$CODE_BASE_PATH/stream-jobs/user-stream-processor/target/user-stream-processor-1.0.0.jar"
  "$CODE_BASE_PATH/stream-jobs/mentoring-stream-processor/target/mentoring-stream-processor-1.0.0.jar"
  "$CODE_BASE_PATH/metabase-jobs/user-service/target/user-service-1.0.0.jar"
  "$CODE_BASE_PATH/metabase-jobs/user-dashboard-creator/target/user-dashboard-creator-1.0.0.jar"
  "$CODE_BASE_PATH/metabase-jobs/mentoring-dashboard-creator/target/mentoring-dashboard-creator-1.0.0.jar"
)


# Submit jobs to Flink
echo -e "${CYAN}Submitting jobs to Flink for ${MAGENTA}$ENVIRONMENT${CYAN} environment...${NC}"
FLINK_CMD="$FLINK_DIR/bin/flink run -m localhost:8081"

for JOB_PATH in "${JAR_PATHS[@]}"; do
  if [[ -f "$JOB_PATH" ]]; then
    JOB_NAME=$(basename "$JOB_PATH" .jar)
    LOG_FILE="$LOG_DIR/$ENVIRONMENT-$JOB_NAME.log"

    echo -e "${YELLOW}Submitting ${MAGENTA}$JOB_NAME${CYAN} to Flink...${NC}"
    echo -e "${CYAN}Command:${NC} nohup $FLINK_CMD $JOB_PATH > \"$LOG_FILE\" 2>&1 &"
    nohup $FLINK_CMD "$JOB_PATH" > "$LOG_FILE" 2>&1 &

    if [[ $? -eq 0 ]]; then
      echo -e "${GREEN}Success:${NC} Job ${MAGENTA}$JOB_NAME${NC} submitted successfully."
    else
      echo -e "${RED}Error:${NC} Job ${MAGENTA}$JOB_NAME${NC} submission failed. Check log at ${MAGENTA}$LOG_FILE${NC}"
    fi
  else
    echo -e "${RED}Error:${NC} JAR file ${MAGENTA}$JOB_PATH${NC} not found. Skipping."
  fi
done

echo -e "\n${CYAN}Deployment process completed.${NC}"
