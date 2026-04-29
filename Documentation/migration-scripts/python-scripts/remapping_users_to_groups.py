#This script reads the previously extracted JSON, maps users to newly created groups based on group names, and updates their memberships in Metabase.
#It enriches the same JSON file with existing and updated group details along with status information.
#The process is logged and designed to safely handle API inconsistencies and partial failures.

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
        f"metabase_remap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger("metabase_remap_logger")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)

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

def get_latest_extract_file():
    files = [
        f for f in os.listdir(LOG_DIR)
        if f.startswith("existing_user_maps_") and f.endswith(".json")
    ]

    if not files:
        raise Exception("No extraction file found in logs folder")

    files.sort(reverse=True)
    return os.path.join(LOG_DIR, files[0])

def get_session(url, username, password):
    res = requests.post(
        f"{url}/session",
        json={"username": username, "password": password},
        timeout=10
    )
    res.raise_for_status()
    return res.json()["id"]

def get_groups(url, session_id):
    res = requests.get(
        f"{url}/permissions/group",
        headers={"X-Metabase-Session": session_id},
        timeout=10
    )
    res.raise_for_status()
    return res.json()

def get_users(url, session_id):
    res = requests.get(
        f"{url}/user?status=active&limit=200&offset=0",
        headers={"X-Metabase-Session": session_id},
        timeout=10
    )
    res.raise_for_status()
    return res.json()["data"]

def get_memberships(url, session_id):
    res = requests.get(
        f"{url}/permissions/membership",
        headers={"X-Metabase-Session": session_id},
        timeout=10
    )
    res.raise_for_status()

    data = res.json()
    memberships = []

    for _, items in data.items():
        for m in items:
            memberships.append({
                "membership_id": m["membership_id"],
                "user_id": m["user_id"],
                "group_id": m["group_id"]
            })

    return memberships

def remove_membership(url, session_id, membership_id):
    try:
        res = requests.delete(
            f"{url}/permissions/membership/{membership_id}",
            headers={"X-Metabase-Session": session_id},
            timeout=10
        )
        res.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.warning(f"Skipping membership delete {membership_id}: {e}")

def add_user_to_group(url, session_id, user_id, group_id):
    requests.post(
        f"{url}/permissions/membership",
        headers={"X-Metabase-Session": session_id},
        json={"user_id": user_id, "group_id": group_id},
        timeout=10
    ).raise_for_status()

def remap_users(url, session_id, json_file):

    with open(json_file, "r") as f:
        data = json.load(f)

    groups = get_groups(url, session_id)
    users = get_users(url, session_id)

    group_map = {g["name"]: g["id"] for g in groups}
    user_map = {u["email"]: u for u in users}

    memberships = get_memberships(url, session_id)

    for user in data:

        email = user["email"]

        existing_ids = [g["id"] for g in user["groups"]]
        existing_names = [g["name"] for g in user["groups"]]

        new_user = user_map.get(email)

        if not new_user:
            user["status"] = "USER_NOT_FOUND"
            logger.warning(f"User not found: {email}")
            continue

        updated_ids = []
        updated_names = []

        for name in existing_names:
            if name in group_map:
                updated_ids.append(group_map[name])
                updated_names.append(name)

        # Remove old groups
        for m in memberships:
            if m["user_id"] != new_user["id"]:
                continue

            # Skip All Users
            if m["group_id"] == 1:
                continue

            # Skip system/admin protected groups (optional safety)
            if m["group_id"] == 2:  # Administrators
                continue

            remove_membership(url, session_id, m["membership_id"])

        # Add new groups
        for gid in updated_ids:
            if gid != 1:
                try:
                    add_user_to_group(url, session_id, new_user["id"], gid)
                except Exception as e:
                    logger.warning(f"Skipping duplicate add for user {new_user['id']}, group {gid}")

        memberships = get_memberships(url, session_id)

        # UPDATE JSON
        user["existing_groups_id"] = existing_ids
        user["existing_groups_name"] = existing_names
        user["updated_groups_id"] = updated_ids
        user["updated_groups_name"] = updated_names
        user["status"] = "SUCCESS"

        logger.info(f"Remapped user: {email}")

    with open(json_file, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Updated JSON: {json_file}")

def main():
    try:
        url, username, password = load_metabase_config()

        session_id = get_session(url, username, password)

        json_file = get_latest_extract_file()

        logger.info(f"Using extract file: {json_file}")

        remap_users(url, session_id, json_file)

        logger.info("Remap pipeline completed successfully")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()