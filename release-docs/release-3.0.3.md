# Release v3.0.3 Deployment Guide

Introduction
This release document provides a comparison between release-2.1.3-hotfix and release-3.0.3. It highlights the new features, bug fixes, and operational improvements introduced in this version.
---

## Summary of Changes
The 3.0.3 release introduces significant enhancements to the Mentoring capabilities, including new Flink jobs for stream processing and dashboard creation. It also brings improvements to the deployment process with multi-architecture Docker images (amd64/arm64) and new data cleanup scripts.

### Key Highlights
- **Mentoring Stream Processor**: A new Flink job has been added to handle mentoring stream processing.
- **Mentoring Dashboard Creator**: A new job to create mentoring dashboards.
- **Docker Improvements**: Support for building Docker images for both `amd64` and `arm64` architectures.
- **Data Cleanup**: Introduction of deletion scripts for metadata, dashboards, and group IDs.
- **Bug Fixes**: Resolution of various issues including CodeRabbit comments, user-metrics queries, and QA reported bugs.

## Required config changes

### 1. Docker compose file changes

- **elevate-data Image Tag:** : `v3.0.3`
- **Rebuild the elevate data docker container**

### 2. Migration scripts
**(Update the common-config.env)**
```
# Logs
export LOG_FILE="/home/user2/Documents/elevate-data/data-pipeline/Documentation/migration-scripts/orgid_update.log"

# Postgres Config
export PGHOST="localhost"
export PGPORT="5432"
export PGDBNAME="test"
export PGUSER="postgres"
export PGPASSWORD="postgres"

# Programs DB Find API Config
export API_URL="https://qa.elevate-apis.shikshalokam.org/project/v1/admin/dbFind/programs"
export AUTH_TOKEN="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9."
export APP_NAME="elevatedata"

# Table Names
export SOLUTION_TABLE="local_solutions"
```
```json
./Documentation/migration-scripts/alter-solution-table.sh
```
### 2. Metabase column data type changes 

When non-English named programs were created, the metabase created the slug name based on the collection name to an encoded format, which extends the column data storage limit. Hence, we need to update the metabase collection table slug column data type from character varying to text.

```
ALTER TABLE collection
ALTER COLUMN slug TYPE TEXT;
```
### 3. Update the dashboard report config
- **First delete all the rose from the {ENV}_report_config table**
```
delete from {ENV}_report_config;
```
- **Reload all the configs by running the data-loader.sh script but first make the necessary config chnages**
```json
# Database connection parameters
DB_NAME="postgres"
DB_USER="postgres"
DB_PASSWORD="postgres"
DB_HOST="localhost"
DB_PORT="5432"
TABLE_NAME="local_report_config"
        
# Json file directory path from inside contianer
MAIN_FOLDER="/app/data-pipeline/metabase-jobs/config-data-loader/projectJson"
```

### 4. Data clean up (If required)
- To setup the data cleanup script follow this doc : Documentation/data-cleanup/readme.md
- Once setup is completed update the config in the Documentation/data-cleanup/program_deletion.py script and trigger it.
```
LOGIN_URL = "https://qa.elevate-apis.shikshalokam.org/user/v1/admin/login"
DB_FIND_URL = "https://qa.elevate-apis.shikshalokam.org/project/v1/admin/dbFind/programs"

USERNAME = "xyz@shikshalokam.com"
PASSWORD = "Password@1234"

# List of required program IDs for which data must be kept; all other program data will be deleted
list_of_required_program_ids = ['692818e8b1b253ba1c3862de','6927ddbeee1c0bba1263db5d','6927f73eee1c0bba1263ddfa',
'68cbd82038aee0086ee6188b','690cc46eec717c3ae4e8f972','690c4ad1aea85109330b201b','690cd3d65b1be23aee52f87b']
```
```
python3 Documentation/data-cleanup/program_deletion.py
```
---