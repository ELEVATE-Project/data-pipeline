#This script connects to Metabase using the configured credentials, retrieves all existing groups and their user memberships, and removes them automatically.
#It safely skips system groups like “All Users” and “Administrators,” deletes associated memberships first, and then deletes the groups.
#All actions are logged with file and console logging for traceability.

import requests
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
        f"metabase_group_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger("metabase_group_cleanup")
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

    return logger


logger = setup_logger()

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
        requests.delete(
            f"{url}/permissions/membership/{membership_id}",
            headers={"X-Metabase-Session": session_id},
            timeout=10
        ).raise_for_status()
    except Exception as e:
        logger.warning(f"Skipping membership delete {membership_id}: {e}")

def delete_group(url, session_id, group_id):
    try:
        requests.delete(
            f"{url}/permissions/group/{group_id}",
            headers={"X-Metabase-Session": session_id},
            timeout=10
        ).raise_for_status()

        logger.info(f"Deleted group: {group_id}")

    except Exception as e:
        logger.error(f"Failed deleting group {group_id}: {e}")

def cleanup_groups(url, session_id):

    groups = get_groups(url, session_id)
    memberships = get_memberships(url, session_id)

    # Protected groups
    protected_groups = {1, 2}

    # Map group -> memberships
    group_members_map = {}

    for m in memberships:
        group_members_map.setdefault(m["group_id"], []).append(m)

    for g in groups:
        gid = g["id"]
        gname = g["name"]

        # Skip system groups
        if gid in protected_groups:
            logger.info(f"Skipping protected group: {gname}")
            continue

        logger.info(f"Processing group: {gname} ({gid})")

        # Remove all memberships first
        for m in group_members_map.get(gid, []):
            remove_membership(url, session_id, m["membership_id"])

        # Delete group
        delete_group(url, session_id, gid)

def main():
    try:
        url, username, password = load_metabase_config()

        session_id = get_session(url, username, password)

        cleanup_groups(url, session_id)

        logger.info("Group cleanup completed successfully")

    except Exception as e:
        logger.exception(f"Pipeline failed: {e}")


if __name__ == "__main__":
    main()