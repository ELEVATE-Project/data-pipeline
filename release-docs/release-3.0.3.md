# Release v3.0.3 Deployment Guide

## Introduction
This release document provides a comparison between release-2.1.3-hotfix and release 3.0.0, 3.0.1, 3.0.2, and 3.0.3. It highlights the new features, bug fixes, and operational improvements introduced in this version.
---

## Summary of Changes
The 3.0.3 release introduces significant enhancements to the Mentoring capabilities, including new Flink jobs for stream processing and dashboard creation. It also brings improvements to the deployment process with multi-architecture Docker images (amd64/arm64) and new data cleanup scripts.

### Key Highlights
- **Mentoring Stream Processor**: A new Flink job has been added to handle mentoring stream processing.
- **Mentoring Dashboard Creator**: A new job to create mentoring dashboards.
- **Docker Improvements**: Support for building Docker images for both `amd64` and `arm64` architectures.
- **Data Cleanup**: Introduction of deletion scripts for metadata, dashboards, and group IDs.
- **Bug Fixes**: Resolution of QA reported bugs.

## Detailed Changelog

### Release 3.0.0
#### Mentoring Dashboards
- **Tenant Admin Dashboard**: Provides an overview of the entire tenant with comparative insights across all organizations under it.
- **Org Admin Dashboard**: Offers organization-level mentoring insights with filters for state, district, block, cluster, and school to enable detailed analysis.

#### Bug Fixes / Enhancements
- Addressed issues observed during the production release:
- Update in `repush-user-kafka-messages.sh` → `created_by: ($created_by | tonumber)`
- Code changes in User Stream Job → Query modifications for resync filters
- Code changes in Project Dashboard Job → Query updates related to tenant_id
- Updated Kafka groupId for user-stream-job in SaaS/Elevate QA environment
- Fixed Org Issue: Multiple program folder collections were being created when the same program was rolled out to more than one organization.
- Added mapping flow for Tenant Admin in the User Activity Dashboard.

### Release 3.0.1
#### Enhancements to the Mentoring Dashboard
- **Program collection**: Now consists of the creator organisation ID in the name.
- Total mentoring hours (big number)
- Scheduled sessions per week (line chart)
- Scheduled sessions per month (line chart)
- Sessions created (bar chart)
- Sessions attended (bar chart)

### Release 3.0.2
- Setup the dashboard deletion stream pipeline

### Release 3.0.3
- Implemented two separate LATERAL UNNEST operations on `task_report` table.
- Fixed issue where School column was missing in observation dashboards.

## Required config changes

### 1. Docker compose file changes

- **elevate-data Image Tag:** : `v3.0.3`
- **Rebuild the elevate data docker container**

### 2. Migration scripts
- Update the common-config.env and trigger the alter-solution-table.sh script.
```JSON
# Postgres Config
export PGHOST="{{PGHOST}}"
export PGPORT="{{PGPORT}}"
export PGDBNAME="{{PGDBNAME}}"
export PGUSER="{{PGUSER}}"
export PGPASSWORD="{{PGPASSWORD}}"

# Programs DB Find API Config
export API_URL="{{DB_FIND_URL}}"
export AUTH_TOKEN="{{access_token}}"
export APP_NAME="{{APP_NAME}}"

# Table Names
export SOLUTION_TABLE="{{ENV}}_solutions"
```
```json
./Documentation/migration-scripts/shell-scripts/alter-solution-table.sh
```
### 3. Metabase column data type changes 

When programs with non-English names are created, Metabase generates the slug based on the collection name in an encoded format, which exceeds the column’s data storage limit. Therefore, we need to update the slug column in the Metabase database collection table. Once the matabase container is up and running export the folloing keys-values and run the bellow command in elevate-data container. 

```
export MB_DB_HOST={postgres_host}
export MB_DB_USER={postgres_user}
export MB_DB_PASS={postgres_password}
export MB_DB_DBNAME={database-name-used-for-metabase}
```
Note : Please export the above keys and values before you run the bellow command. Run this command in the elevate-data container
```
PGPASSWORD="$MB_DB_PASS" psql -h "$MB_DB_HOST" -U "$MB_DB_USER" -d "$MB_DB_DBNAME" -c \
"ALTER TABLE collection ALTER COLUMN slug TYPE TEXT USING slug::TEXT;"
```

### 4. Update the dashboard report config
- **First delete all the rows from the {ENV}_report_config table**
- {ENV} example local, dev, qa and prod
```
delete from {ENV}_report_config;
```
- **Reload all the configs by running the data-loader.sh script but first make the necessary config changes**
```json
# Database connection parameters
DB_NAME="{{PGDBNAME}}"
DB_USER="{{PGUSER}}"
DB_PASSWORD="{{PGPASSWORD}}"
DB_HOST="{{PGHOST}}"
DB_PORT="{{PGPORT}}"
TABLE_NAME="{{ENV}}_report_config"
        
# Json file directory path from inside container
MAIN_FOLDER="/app/data-pipeline/metabase-jobs/config-data-loader/projectJson"
```

### 5. Normalize org_code in all the tables 
- To maintain consistency across all tables, we need to normalize the organization codes by converting them to lowercase and replacing any spaces with underscores. This will ensure that the org_code is uniform across all tables and can be easily referenced in queries and dashboards.
- Update the common-config.env and trigger the update-org-code.sh script.
- Dry run the below command to update the org_code in all the tables:
```shell 
  ./Documentation/migration-scripts/update-org-code.sh --dry-run  
```
- If the dry run looks good, run the below command to update the org_code in all the tables:
```shell
  ./Documentation/migration-scripts/update-org-code.sh
```


### 6. Data clean up (If required)
- To setup the data cleanup script follow this doc : Documentation/data-cleanup/python-script/resource_delete.md
- Once setup is completed update the config in the config.ini and run the Documentation/data-cleanup/python-script/program_deletion.py script.

```json
login_url = "{{ACCESS_TOKEN_GENERATION_URL}}"
db_find_url = "{{DB_FIND_URL}}"
username = "{{ADMIN_USERNAME}}"
password = "{{ADMIN_PASSWORD}}"
list_of_required_program_ids = "{{PASTE_HERE_COMMA_SEPARATED_LIST_OF_PROGRAM_IDS_THAT_MUST_BE_KEPT}}"
```
```
python3 Documentation/data-cleanup/python-script/program_deletion.py
```
### 7. Recreation of All Dashboards
- To reflect the updated tables and charts in the dashboards, all dashboards need to be recreated. 
- Please follow the documentation: ./Documentation/migration-scripts/python-scripts/dashboard_recreation.md
---
