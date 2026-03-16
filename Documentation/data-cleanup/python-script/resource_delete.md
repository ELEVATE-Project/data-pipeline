# Resource Deletion Script Documentation

Referred Script: `Documentation/data-cleanup/python-script/resource_delete.py`

## Overview
This script is a Kafka consumer that listens for deletion events on a specific topic (`RESOURCE_DELETION_TOPIC`) and performs cleanup operations in the database and Metabase dashboards and GroupIds. It handles cleanup for both **Programs** and **Solutions**.

## Logic Flow

### 1. Initialization and Configuration
- **Load Configuration**: Reads database and Metabase credentials from `config.ini`.
- **Logging**: Sets up logging to both a file (`resource_delete.log`) and the console.
- **Database Connection**: Establishes a connection to the PostgreSQL database.
- **Kafka Consumer**: Initializes a Kafka consumer to listen to the specified topic.

### 2. Message Processing (Main Loop)
The script continuously listens for messages from Kafka. For each message:
1.  **Validation**: Checks if the message contains `type` and `entityId`. If missing, it logs an error and skips the message.
2.  **Entity Type Check**: Determines if the entity is a `program` or a `solution`.

---

### 3. Deletion Logic

#### Case A: Program Deletion (`type="program"`)
When a program deletion event is received:
1.  **Identify Associated Solutions**:
    - Queries the database (`{ENV}_solutions` table) to find all `solution_id`s linked to the given `program_id`.
2.  **Process Each Solution**:
    - Iterates through each found `solution_id`.
    - Tries to process it as an **Improvement Project**.
    - If it is *not* an improvement project, it attempts to process it as a **Survey** or **Observation**.
3.  **App Level Cleanup**:
    - Deletes metadata from `{ENV}_dashboard_metadata`.
4.  **Metabase Cleanup**:
    - Deletes the associated Collection in Metabase.
    - Deletes the associated Permission Group in Metabase.

#### Case B: Solution Deletion (`type="solution"`)
When a solution deletion event is received:
1.  **Process Solution**:
    - Tries to process it as an **Improvement Project**.
    - If it is *not* an improvement project, it attempts to process it as a **Survey** or **Observation**.
2.  **Metabase Cleanup**:
    - Deletes the associated Collection in Metabase.
    - Deletes the associated Permission Group in Metabase.

---

### 4. Detailed Processing Logic

#### `process_improvement_project(solution_id)`
- Checks if the solution is linked to any projects in the `{ENV}_projects` table.
- **If Projects Exist**:
    - Deletes related tasks from `{ENV}_tasks`.
    - Deletes the project from `{ENV}_projects`.
    - Deletes the solution from `{ENV}_solutions`.
    - Deletes dashboard metadata from `{ENV}_dashboard_metadata`.
    - Returns `True` (indicating it was an improvement project).
- **If No Projects Found**: Returns `False`.

#### `process_observation(solution_id)` / `process_survey(solution_id)`
These functions handle cleanup for Observation and Survey solutions respectively by attempting to drop specific dynamic tables if they exist.

- **Observation Tables Checked**:
    - `{solution_id}_domain`
    - `{solution_id}_status`
    - `{solution_id}_questions`
- **Survey Tables Checked**:
    - `{solution_id}_survey_status`
    - `{solution_id}`

#### `drop_if_exists(table_name, solution_id)`
Helper function used by observation/survey processors:
- Checks if `table_name` exists in the database.
- If it exists:
    - Drops the table.
    - Deletes the solution entry from `{ENV}_solutions`.
    - Deletes metadata from `{ENV}_dashboard_metadata`.

---

## Configuration Variables
The script relies on the following variables from `config.ini`:
- **Database**: `host`, `user`, `password`, `dbname`, `env`, `topic`, `group_id`, `broker`
- **Metabase**: `url`, `username`, `password` (loaded via helper)

## Dependencies
- `psycopg2`: For PostgreSQL database interactions.
- `kafka`: For consuming messages from Kafka.
- `dashboard_resource_helper`: For Metabase API interactions.

---

## Helper Module: `dashboard_resource_helper.py`

This module contains the `MetabaseUtil` class, which handles all interactions with the Metabase API.

### `MetabaseUtil` Class Flow

#### 1. Initialization
- **`__init__(metabase_url, username, password)`**: Stores credentials and initializes session state and cache.

#### 2. Authentication
- **`get_session_token()`**:
    - Checks if a valid session token already exists.
    - If not, sends a POST request to `{metabase_url}/session` with credentials.
    - Returns the session ID to be used in subsequent headers.

#### 3. Collection Management
- **`list_collections(force_refresh=False)`**:
    - Fetches all collections from `{metabase_url}/collection`.
    - Implements caching to reduce API calls.
- **`get_collection_id(target_id)`**:
    - Iterates through all collections.
    - Matches if the `target_id` (Program ID or Solution ID) is present in the collection's **description**.
    - Returns a list of matching Collection IDs.
- **`delete_collection(collection_id)`**:
    - Accepts a list of Collection IDs.
    - Sends a PUT request to `{metabase_url}/collection/{id}` with `{"archived": True}`.
    - Logs success or failure.

#### 4. Permission Group Management
- **`get_permission_groups()`**:
    - GETs all groups from `{metabase_url}/permissions/group`.
- **`get_permission_group_id(groups, program_id)`**:
    - Static method helper.
    - Searches the provided list of groups.
    - Returns the Group ID if the `program_id` (or Solution ID) matches the group **name**.
- **`delete_permission_group(group_id)`**:
    - Sends a DELETE request to `{metabase_url}/permissions/group/{group_id}`.
    - Logs success or failure.


---

## Setup & Run with tmux

### Step 1: Install tmux (if not installed)
```bash
sudo apt install tmux -y
```
Check version:
```bash
tmux -V
```

### Step 2: Create a New tmux Session
```bash
tmux new -s resource_cleanup
```
Now you are inside the tmux session.

### Step 3: Run Your Python Script
Use full python path (recommended):
```bash
python3 /app/Documentation/data-cleanup/python-script/resource_delete.py
```

### Step 4: Detach from tmux (Leave It Running)
Press: `Ctrl + B` then `D`
This will detach the session but keep your script running in background.

### Step 5: Reattach Later
To come back:
```bash
tmux attach -t resource_cleanup
```

### Step 6: Check Running Sessions
```bash
tmux ls
```

### Step 7: Kill the Session (Stop Script) If required
```bash
tmux kill-session -t resource_cleanup
```
