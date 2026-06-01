# Metabase User Group Automation Pipeline

## Overview

This script automates the complete lifecycle of Metabase user-group migration and remapping.

It combines three operations into a single execution-based pipeline:

1. Fetch existing user-group mappings
2. Delete existing groups
3. Remap users to newly created groups

The execution mode is passed via command-line arguments.

---

# Features

* Single unified automation script
* Command-line execution modes
* Automatic logging to file and console
* JSON snapshot generation
* Automatic user remapping
* Uses unified-common.conf for credentials
* Updates JSON with remapped group details
* Preserves protected groups (`All Users`, `Administrators`)

---

# Supported Execution Modes

| Mode                    | Description                                    |
| ----------------------- | ---------------------------------------------- |
| `fetch_user_details`    | Fetch all active users and their mapped groups |
| `delete_all_the_groups` | Delete all non-system Metabase groups          |
| `remap_the_users`       | Remap users to newly created groups            |



---

# Configuration

Ensure `unified-common.conf` contains:

```hocon
metabase {
  url = "http://localhost:3000/api"
  username = "your_username"
  password = "your_password"
}
```

---

# Environment Variable

Set:

```bash
export UNIFIED_PIPELINE_CONF=/path/to/unified-common.conf
```

Optional:

```bash
export DATA_PIPELINE_ROOT=/path/to/data-pipeline
```

---

# Usage

## 1. Fetch Existing User Mappings

```bash
python3 fetching_and_auto_mapping_groups.py fetch_user_details
```

This generates:

```text
logs/existing_user_maps_YYYY-MM-DD.json
logs/metabase_fetch_user_details_YYYY-MM-DD.log
```

---

## 2. Delete Existing Groups

```bash
python3 fetching_and_auto_mapping_groups.py delete_all_the_groups
```
This generates:

```text
logs/metabase_delete_all_the_groups_YYYY-MM-DD.log
```
Protected groups are skipped:

* All Users
* Administrators

---

## 3. Recreate All The Dashboards 

Please use this [document](Documentation/migration-scripts/python-scripts/dashbard_recreation.md)  to recreate the dashboards once the group ID has been deleted.


---

## 4. Remap Users

After recreating dashboards/groups:

```bash
python3 fetching_and_auto_mapping_groups.py remap_the_users
```
This generates:

```text
logs/metabase_remap_the_users_YYYY-MM-DD.log
```

The existing JSON file is updated with:

* existing group IDs
* existing group names
* updated group IDs
* updated group names
* remap status

---

# Sample JSON Output

```json
{
  "user_id": 7,
  "name": "User Name",
  "email": "user@example.com",
  "groups": [
    {
      "id": 1,
      "name": "All Users"
    }
  ],
  "existing_groups_id": [1, 45],
  "existing_groups_name": ["All Users", "Program_Manager"],
  "updated_groups_id": [1, 67],
  "updated_groups_name": ["All Users", "Program_Manager"],
  "status": "SUCCESS"
}
```

---


---

# Important Notes

* Group names must remain consistent for remapping
* Memberships are automatically removed when groups are deleted
* Duplicate membership additions are safely skipped
* Remap uses email as the primary user identifier

---

# Recommended Execution Flow

```text
1. fetch_user_details
2. delete_all_the_groups
3. recreate dashboards/groups (execute this script to populate dashboards and groups : push_kafka_messages.py)
4. remap_the_users
```

---

# Error Handling

The script safely handles:

* missing users
* missing groups
* duplicate memberships
* invalid configs
* API failures

Errors are logged without stopping the entire pipeline.

---

# Summary

This utility automates Metabase user-group migration, deletion, and remapping while maintaining auditability through JSON snapshots and logs.
