# Release Notes – v3.0.1
## 1. Overview

### This release includes major improvements to the Mentoring Dashboards, enhancements in scheduled session metrics, updates in batch processing, and multiple fixes identified during the previous production rollout.
### It covers both UI and backend changes using Docker-based deployment.

## 2. Changes Since v2.1.3-hotfix
### (I) Mentoring Dashboards

* Introduced Tenant Admin Dashboard with cross-organization insights and comparative analytics.

* Introduced Org Admin Dashboard with detailed organization-level mentoring metrics and filters for:
state, district, block, cluster, school.

### (II) New Enhancements

* Added new metrics for Scheduled and Published Sessions.

* Added start_date and end_date extraction in the Python Batch Script.

### (III) Bug Fixes / Improvements

* Fixed issue where multiple program folder collections were created when the same program rolled out to multiple organizations. [[Ticket #3988](https://katha.shikshalokam.org/bug-view-3988.html)]

* Updated repush-user-kafka-messages.sh →
created_by: ($created_by | tonumber) [[PR-134]](https://github.com/ELEVATE-Project/data-pipeline/pull/134)

* Updated User Stream Job queries for filter resync. [[PR-134]](https://github.com/ELEVATE-Project/data-pipeline/pull/134)

* Updated Project Dashboard Job for tenant_id query fix. [[PR-134]](https://github.com/ELEVATE-Project/data-pipeline/pull/134)

* Updated Kafka groupId for the user-stream-job.

* Added mapping flow for Tenant Admin in the User Activity Dashboard.

## 3. Pre-Deployment Checklist

### Perform these steps before deploying v3.0.1:

### Environment Readiness

* Verify the correct branch is checked out and merged (release/v3.0.1 or equivalent).

* Confirm all Dockerfile updates are committed.

* Ensure .env or environment variables are updated (Kafka groupId, DB credentials, etc.).

### Database Backup & Validation

* Take DB backup for safety (at minimum: user, mentoring, dashboard-related tables).

* Validate new SQL queries on QA DB before deployment.

* Confirm that start_date and end_date fields exist and are correct in the sessions table.

### Dashboard Preparation

* Clean/remove existing Mentoring Dashboards if required (to avoid duplicate cards/collections).

* Confirm tenant/org IDs are correct for dashboard creation job.

* Ensure Metabase credentials & API keys are active.

### Docker Readiness

* Ensure Docker daemon is running and has enough disk space.

* Validate Docker build locally on QA branch.

* Test pushing and pulling images from registry.

## 4. Deployment Steps (Docker-Based Deployment)
### Build the Docker Image
   ```docker build -t elevate-data:3.0.1 .```

### Tag & Push to Registry :
```
docker tag elevate-data:3.0.1 <REGISTRY_URL>/elevate-data:3.0.1
docker push <REGISTRY_URL>/elevate-data:3.0.1
```
* Update Deployment Files

* Update version tag in: ```docker-compose.yml```


#### Example:

```image: <REGISTRY_URL>/elevate-data:3.0.1```

 **Update the conf files.**
*  - ./config_files/mentoring-stream.conf:/app/stream-jobs/mentoring-stream-processor/src/main/resources/mentoring-stream.conf
*  - ./config_files/metabase-mentoring-dashboard.conf:/app/metabase-jobs/mentoring-dashboard-creator/src/main/resources/metabase-mentoring-dashboard.conf
* These are mentoring stream job and mentoring dashboard job configuration files respectively. Make sure to update any necessary parameters inside these files as per the new release requirements.
* These files are mounted as volumes in the docker-compose.yml file. Under the Elevate-Data Container section, ensure the paths are correctly specified respectively.
* Also increase the Task Manager slots from 9 to 11 in the docker-compose.yml file.
#### Apply Deployment

```docker-compose up -d```

#### Submitting the Flink Jobs
* Go to the Elevate-Data container, inside the /app directory run the following commands to build the jar.

```mvn clean install -DskipTests```
* Once the .jars are build successfully then trigger the submitjobs.sh script to submit the flink jobs.

```./app/Documentation/Docker-setup/submit-jobs.sh  jobmanager```
## 5. Post-Deployment Checklist
### Dashboard Validation

* Verify Tenant Admin & Org Admin dashboards load correctly.

* Check new scheduled session metrics card.

* Validate filter mappings (state, district, block, cluster, school).

* Ensure no duplicate collections were created.

### Data Validation

* Confirm that batch script processed start_date & end_date.

* Review scheduled session line-chart values for next 6 months.

### Service Validation

* Validate logs for Mentoring Dashboard Creator job.

* Check Kafka consumption using updated groupId.

* Ensure no duplicate or stuck messages.

### Stability Check

* Monitor system for 15–30 min post-release.

* Validate API responses from mentoring & user services.

## 6. Rollback / Reversion Steps

### If deployment fails:

### Step 1 — Revert to Previous Docker Image

* Update tag back to v2.1.3-hotfix:

```image: <REGISTRY_URL>/elevate-data:2.1.3-hotfix```

**Apply:**

```docker-compose up -d```

#### Submitting the Flink Jobs
* Go to the Elevate-Data container, inside the /app directory run the following commands to build the jar.

```mvn clean install -DskipTests```
* Once the .jars are build successfully then trigger the submitjobs.sh script to submit the flink jobs.

```./app/Documentation/Docker-setup/submit-jobs.sh  jobmanager```

### Step 2 — Restore Backup

**If DB changes were applied:**

* Restore the latest DB backup taken in pre-deployment steps.

### Step 3 — Revert Dashboard State

* Drop newly created Metabase collections/cards if needed.

* Re-run previous stable dashboard creation job if applicable.