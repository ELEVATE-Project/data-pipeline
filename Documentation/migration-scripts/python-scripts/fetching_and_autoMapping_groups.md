# Metabase User–Group Automation Pipeline

## 📌 Overview

This script automates the complete lifecycle of user-group management in Metabase. It combines three operations into a single configurable pipeline:

1. **Fetch User Details** – Extract existing users and their group mappings
2. **Delete Groups** – Remove existing groups and their memberships
3. **Remap Users** – Reassign users to newly created groups based on previous mappings

The script is controlled using a single execution mode and leverages a unified configuration file for credentials.

---

## ⚙️ Features

* Single script for all operations (fetch, delete, remap)
* Execution controlled via `EXECUTION_MODE`
* Automatic logging (file + console)
* JSON snapshot storage for audit and reuse
* Safe handling of:

    * Protected groups (`All Users`, `Administrators`)
    * Duplicate mappings
    * Missing users/groups
* Updates JSON with:

    * Existing group IDs & names
    * Updated group IDs & names
    * Execution status


---

## 🚀 Execution Modes

Set the mode inside the script:

```python
EXECUTION_MODE = "fetch_user_details"
```

### Available Modes:

| Mode                    | Description                         |
| ----------------------- | ----------------------------------- |
| `fetch_user_details`    | Extract users and group mappings    |
| `delete_all_the_groups` | Remove all non-system groups        |
| `remap_the_users`       | Remap users to newly created groups |

---

## 🔄 Workflow

Typical execution flow:

```text
1. fetch_user_details
2. delete_all_the_groups
3. (Recreate dashboards & groups externally)
4. remap_the_users
```

---

## 📊 Output

### 1. Logs

Stored in:

```
logs/metabase_pipeline_YYYYMMDD_HHMMSS.log
```

### 2. JSON Snapshot

```
logs/existing_user_maps_YYYY-MM-DD.json
```

### Sample Output:

```json
{
  "user_id": 7,
  "name": "User Name",
  "email": "user@example.com",
  "groups": [...],
  "existing_groups_id": [...],
  "existing_groups_name": [...],
  "updated_groups_id": [...],
  "updated_groups_name": [...],
  "status": "SUCCESS"
}
```

---

## ⚠️ Important Notes

* Group names must remain consistent for remapping to work
* System groups are never deleted:

    * `All Users`
    * `Administrators`
* Duplicate group assignments may return `500` from Metabase — handled safely
* Script assumes dashboards/groups are recreated before remap step

---

## 🛠️ Error Handling

* Skips invalid users/groups with warnings
* Handles API inconsistencies gracefully
* Continues execution even if individual mappings fail

---

## 💡 Best Practices

* Always run **fetch before delete**
* Verify groups are recreated before remap
* Keep logs for debugging and audit
* Use same naming convention for groups


## ✅ Summary

This script provides a **fully automated, reliable, and scalable solution** for managing user-group mappings in Metabase, eliminating manual effort and ensuring consistency across environments.
