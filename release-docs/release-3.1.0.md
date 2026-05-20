# 🚀 ELEVATE Data Pipeline — Release Notes `v3.1.0`

<div align="center">

### Major Platform Enhancements • Reliability Improvements • Flink Optimization • Metabase Automation

![Release](https://img.shields.io/badge/Release-v3.1.0-blue?style=for-the-badge)
![Platform](https://img.shields.io/badge/Platform-ELEVATE_Data_Pipeline-orange?style=for-the-badge)
![Status](https://img.shields.io/badge/Status-Stable-success?style=for-the-badge)

</div>

---

# 📌 Overview

Release **`v3.1.0`** focuses on improving the overall reliability, scalability, maintainability, and operational efficiency of the ELEVATE Data Pipeline ecosystem.

This release introduces:

- ⚡ Automated Flink Job Management
- 🩺 Health Check Monitoring
- 📊 Structured Logging Framework
- 🧩 Centralized & Dynamic Configuration
- 🐳 Docker Infrastructure Enhancements
- 🤖 Metabase Automation Improvements
- 🔄 CI/CD Enhancements
- 🚀 Performance & Resource Optimization

---

# ✨ Release Highlights

| Area | Enhancements |
|------|--------------|
| ⚙️ Flink | Auto Job Submission, Resource Optimization, Dynamic Config Loading |
| 🩺 Monitoring | Health Check Monitoring |
| 📊 Logging | Structured Logging Framework |
| 🐳 Docker | Improved Container Orchestration |
| 🤖 Metabase | Dashboard Automation & API Optimization |
| 🔄 CI/CD | Docker Image CI + CodeRabbit Integration |
| 📨 Kafka Utilities | Shell Scripts Migrated to Python |
| 🔐 Access Control | Improved dashboard mapping and group ID cleanup |

---

# ⚙️ Flink Platform Enhancements

## 🚀 Automated Flink Job Submission

Introduced automated Flink job submission during container startup.

### ✅ Improvements

- Automatic job startup during deployment
- Reduced manual operational effort
- Better deployment consistency
- Improved orchestration flow

### 🔁 Previous Approach
#### Previously, this script required manual execution from within the container.

```bash
submit-jobs.sh
```

### ✅ New Approach
#### The script is now integrated as the container entrypoint, automatically monitoring job status and restarting Flink jobs if they stop.

```bash
python3 elevate-data-entrypoint.py
```

This new orchestration flow also improves monitoring and service lifecycle management.

---

# 🩺 Health Check Monitoring

Comprehensive health monitoring support has been added for platform services.

## 🔍 Services Covered

- Flink JobManager
- Flink TaskManager
- Kafka
- Metabase

## ✅ Benefits

- Faster issue detection
- Improved operational visibility
- Easier production monitoring
- Better recovery handling

---

# 📊 Structured Logging Framework

Introduced a centralized structured logging framework across Flink jobs and supporting services.

## ✨ Improvements

- Standardized log formatting
- Better debugging capabilities
- Easier exception tracking
- Improved service traceability
- Persistent log storage support

## 📁 Logging Support

Dedicated log persistence added through Docker volume mounts.

```yaml
- ../../logs:/app/logs
- ../../logs/job-logs/:/opt/flink/log
```

---

# ⚡ Flink Job Restructuring

Flink processing architecture was restructured to improve cluster efficiency and resource utilization.

## ✅ Enhancements

- Better task slot utilization
- Reduced execution overhead
- Cleaner job organization
- Improved scalability
- More maintainable stream processing structure

## 🎯 Outcome

- Improved runtime stability
- Better cluster performance
- Reduced operational complexity

---

# 🧩 Centralized Configuration Management

Configuration management was consolidated into a unified shared configuration model.

---

## ❌ Previous Structure

Multiple independent configuration files:

```yaml
- base-config.conf
- project-stream.conf
- survey-stream.conf
- observation-stream.conf
- user-stream.conf
- mentoring-stream.conf
```

---

## ✅ New Structure

Unified centralized configuration:

```yaml
- ../../unified-common.conf:/app/unified-common.conf
```

---

## 🚀 Benefits

- Reduced configuration duplication
- Easier environment management
- Simplified deployments
- Cleaner maintenance process
- Shared configuration consistency

---

# 🔄 Dynamic Configuration Loading

Dynamic configuration loading support was added for Flink jobs.

## ✨ Improvements

- Runtime-friendly configuration handling
- Easier environment updates
- Improved deployment flexibility
- Reduced static configuration dependency

---

# 📨 Kafka Utility Script Migration

Kafka utility scripts were migrated from Shell scripts to Python.

## 🔁 Migrated Scripts

- `push-kafka-messages.sh`
- `repush-user-kafka-messages.sh`

---

## ✅ Benefits

- Better maintainability
- Improved error handling
- Cleaner logging support
- Easier extensibility
- Improved execution control

---

# 🤖 Metabase Improvements

---

## 🚫 Prevent Private Program Dashboard Generation

Logic introduced to prevent unintended private dashboard creation.

### ✅ Benefits

- Cleaner dashboard hierarchy
- Better governance
- Reduced dashboard duplication
- Improved access control consistency

---

## ⚡ Reduced Dependency on Metabase Collection APIs

Several Metabase operations were optimized by replacing API calls with direct database interactions.

### 🚀 Improvements

- Faster execution
- Reduced API dependency
- Lower latency
- Improved scalability

### 🔧 Optimized Areas

- Collection validation
- Dashboard lookup
- Metadata retrieval
- Mapping operations

---

## 👥 Automated User Mapping to Dashboards

Introduced automation for Metabase group remapping and dashboard assignments.

### ✨ Features

- Fetch existing users
- Remove outdated groups
- Recreate mappings automatically
- Reassign dashboard access

### ✅ Benefits

- Reduced manual effort
- Faster onboarding
- Easier permission corrections
- Improved dashboard consistency

---

# 🏷️ Program Collection Naming Improvements

Program collection naming logic was improved.

## 🔄 Change

The organization name is no longer appended directly to collection names.

### ✅ New Behavior

- Organization information stored in descriptions
- Cleaner naming conventions
- Improved readability in Metabase

---

# 🔄 CI/CD Enhancements

---

## 🤖 CodeRabbit Integration

Added CodeRabbit workflow integration.

### ✅ Benefits

- Automated PR review assistance
- Better code quality checks
- Improved development workflow
- Faster review cycles

---

## 🐳 Continuous Integration for Docker Images

Introduced CI support for Docker image validation and build workflows.

### 🚀 Improvements

- Automated Docker build validation
- Better release consistency
- Improved deployment reliability
- Faster development lifecycle

---

### 🚀 Benefits

- Automated orchestration
- Better lifecycle management
- Improved monitoring support
- Cleaner runtime supervision

---

# 📈 Performance Improvements

## 🚀 Included Optimizations

- Reduced Metabase API overhead
- Faster dashboard operations
- Improved Flink resource utilization
- Better logging efficiency
- Faster startup orchestration
- Cleaner deployment flow

---

# 🔒 Stability & Reliability Improvements

- Improved container startup handling
- Better service dependency management
- Cleaner runtime supervision
- Improved failure recovery
- Better operational monitoring
- Enhanced error handling

---

# 📦 Deployment Notes

## ✅ Recommended Actions

### 1. Configure the unified-common.conf  :
#### update the conf in the file according to the environment .
### 2. enable the jobs which you want to run on the flink :
#### ex . If we want to run only the project, surevy and observation releated stream and dashboard jobs then set the respective config to true .

	combined.project.stream.job.enabled  = true
	combined.survey.stream.job.enabled  = true
	combined.observation.stream.job.enabled  = true
	combined.user.stream.job.enabled  = false
	combined.mentoring.stream.job.enabled  = false
	combined.project.dashboard.job.enabled  = true
	combined.survey.dashboard.job.enabled  = true
	combined.observation.dashboard.job.enabled  = true
	combined.mentoring.dashboard.job.enabled  = false
	combined.user.dashboard.job.enabled  = false
	combined.user.mapping.job.enabled  = true
	combined.program.mapping.job.enabled  = true
### 3. Update Docker Compose configuration :
	- update the unified-common.conf file path at taskmanager and the elevate data volumn section .
	    volumes:
	       /home/local/reports/release-3.1.0/unified-common.conf:/opt/flink/conf/unified-common.conf
	- update the log4j2.properties file path at the taskmanger .
		volumes:
		   /home/local/reports/release-3.1.0/log4j2.properties:/opt/flink/conf/log4j-console.properties
	- update the logs folder file path  at taskmanager and the elevate data volumn section .
	   volumes:
	  /home/local/reports/release-3.1.0/logs/job-logs/:/opt/flink/log

### 4. Recreation of All Dashboards

- Before Re-Creating the Dashboards, make sure you go through this documentation for Fetching the existing user mapping groups of metabase and Re-mapping to newly created Dashboards. [Mapping-Metabase-Groups](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/Documentation/migration-scripts/python-scripts/metabase-group-script/mapping_groups.md)

- To reflect the updated tables and charts in the dashboards, all dashboards need to be recreated.

- Please follow the documentation: [/Documentation/migration-scripts/python-scripts/dashboard_recreation.md](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/Documentation/migration-scripts/python-scripts/dashbard_recreation.md)

---

# 📁 Reference Files

| File                                                                                                                                                    | Description |
|---------------------------------------------------------------------------------------------------------------------------------------------------------|-------------|
| [unified-common.conf](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/unified-common.conf)                                          | Centralized configuration |
| [elevate-data-entrypoint.py](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/Documentation/Docker-setup/elevate-data-entrypoint.py) | Automated orchestration |
| [log4j2.properties](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/log4j2.properties)                                              | Structured logging |
| [docker-compose.yml](https://github.com/ELEVATE-Project/data-pipeline/blob/release-3.1.0/Documentation/Docker-setup/docker-compose.yml)                 | Updated infrastructure setup |

---
