# Postgres Data Cleanup Script for Elevate Data Service

## 📌 Purpose

This script is used to clean up the `elevate_data` PostgreSQL database. It performs the following actions:

- **Deletes** all tables **not** prefixed with `prod_`
- **Truncates** the following production tables:
    - `prod_solutions`
    - `prod_projects`
    - `prod_tasks`
    - `prod_dashboard_metadata`

This helps ensure only required production data is retained, while unnecessary tables and data are removed.

---

## ✅ Prerequisites

Before executing the script, make sure you have:

- PostgreSQL **host/IP address**
- PostgreSQL **port**
- PostgreSQL **user credentials**

- Sufficient privileges to drop and truncate tables

---

## 🚀 How to Run

### Outside a Docker Container

1. Copy the script file `data-cleanup.sh` into the path: `/Shiksha-Lokam/DATA-PIPELINE/`
2. Open the script and update the PostgreSQL configuration values:

```bash
export PGPASSWORD='your-postgres-password'

DB_USER='your-username'
DB_HOST='your-host'
DB_PORT='your-port'
DB_NAME='elevate_data'
```

3. Make the script executable:

```bash
chmod +x data-cleanup.sh
```

4. Run the script:

```bash
./data-cleanup.sh
```

## 📂 What the Script Does

* Terminates all existing connections to the database
* Drops all tables that do not start with prod_
* Truncates production tables listed above
* Logs actions with timestamps to help track what was executed

## ⚠️ Notes

* Take a full backup of the elevate_data database before running this script.
* Make a note of the logs from the terminal. 