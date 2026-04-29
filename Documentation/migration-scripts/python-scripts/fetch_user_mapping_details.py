#This script connects to Metabase using configured credentials, retrieves all active users and groups, and builds a snapshot of user-to-group mappings.
#It stores the extracted data as a JSON file in the logs directory for downstream processing.
#Logging is handled with timestamped files and console output for traceability.

import requests
import json
import os
import sys
from datetime import datetime
from pyhocon import ConfigFactory
import logging

def setup_logger():
    ROOT_DIR = "/home/user-1/Documents/elevate-dev/data-pipeline"
    LOG_DIR = os.path.join(ROOT_DIR, "logs")

    os.makedirs(LOG_DIR, exist_ok=True)

    log_filename = os.path.join(
        LOG_DIR,
        f"metabase_extract_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger("metabase_extract_logger")
    logger.setLevel(logging.INFO)

    # Clear old handlers
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File logging
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

    # Console logging
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, LOG_DIR


logger, LOG_DIR = setup_logger()

def load_metabase_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    unified_conf = os.environ.get(
        "UNIFIED_PIPELINE_CONF",
        os.path.abspath(os.path.join(base_dir, "../../..", "unified-common.conf"))
    )

    config = ConfigFactory.parse_file(unified_conf)

    url = config.get_string("metabase.url")
    username = config.get_string("metabase.username")
    password = config.get_string("metabase.password")

    return url, username, password

def get_session(url, username, password):
    logger.info("Authenticating with Metabase")

    res = requests.post(
        f"{url}/session",   # NO /api added
        json={"username": username, "password": password},
        timeout=10
    )
    res.raise_for_status()

    logger.info("Authentication successful")
    return res.json()["id"]

def get_groups(url, session_id):
    logger.info("Fetching groups")

    res = requests.get(
        f"{url}/permissions/group",
        headers={"X-Metabase-Session": session_id},
        timeout=10
    )
    res.raise_for_status()

    groups = res.json()
    logger.info(f"Fetched {len(groups)} groups")

    return groups

def get_users(url, session_id):
    logger.info("Fetching users")

    res = requests.get(
        f"{url}/user?status=active&limit=100&offset=0",
        headers={"X-Metabase-Session": session_id},
        timeout=10
    )
    res.raise_for_status()

    users = res.json()["data"]
    logger.info(f"Fetched {len(users)} users")

    return users

def build_snapshot(users, groups):
    logger.info("Building user-group snapshot")

    group_map = {
        g["id"]: {"id": g["id"], "name": g["name"]}
        for g in groups
    }

    final_data = []

    for user in users:
        user_groups = []

        for gid in user.get("group_ids", []):
            user_groups.append(
                group_map.get(gid, {"id": gid, "name": "UNKNOWN"})
            )

        final_data.append({
            "user_id": user.get("id"),
            "name": user.get("common_name"),
            "email": user.get("email"),
            "groups": user_groups
        })

    logger.info("Snapshot built successfully")
    return final_data

def save_to_file(data):
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"existing_user_maps_{date_str}.json"

    file_path = os.path.join(LOG_DIR, filename)

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Snapshot saved to {file_path}")

def main():
    try:
        url, username, password = load_metabase_config()

        session_id = get_session(url, username, password)

        groups = get_groups(url, session_id)
        users = get_users(url, session_id)

        snapshot = build_snapshot(users, groups)

        save_to_file(snapshot)

        logger.info("Extraction pipeline completed successfully")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()