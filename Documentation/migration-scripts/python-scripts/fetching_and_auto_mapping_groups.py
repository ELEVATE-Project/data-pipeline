import requests
import json
import os
import sys
from datetime import datetime
from pyhocon import ConfigFactory
import logging

EXECUTION_MODE = "remap_the_users"
VALID_MODES = {
    "fetch_user_details",
    "delete_all_the_groups",
    "remap_the_users"
}

if EXECUTION_MODE not in VALID_MODES:
    raise Exception(f"Invalid EXECUTION_MODE: {EXECUTION_MODE}")

def setup_logger():
    ROOT_DIR = "/home/user-1/Documents/elevate-dev/data-pipeline"
    LOG_DIR = os.path.join(ROOT_DIR, "logs")
    os.makedirs(LOG_DIR, exist_ok=True)

    log_file = os.path.join(
        LOG_DIR,
        f"metabase_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    )

    logger = logging.getLogger("metabase_pipeline")
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    file_handler = logging.FileHandler(log_file)
    console_handler = logging.StreamHandler(sys.stdout)

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger, LOG_DIR


logger, LOG_DIR = setup_logger()

def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    unified_conf = os.environ.get(
            "UNIFIED_PIPELINE_CONF",
            os.path.abspath(os.path.join(base_dir, "../../..", "unified-common.conf"))
    )

    config = ConfigFactory.parse_file(unified_conf)

    return {
        "url": config.get_string("metabase.url"),
        "username": config.get_string("metabase.username"),
        "password": config.get_string("metabase.password")
    }

def get_session(url, username, password):
    res = requests.post(
        f"{url}/session",
        json={"username": username, "password": password}
    )
    res.raise_for_status()
    return res.json()["id"]


def get_groups(url, session_id):
    return requests.get(
        f"{url}/permissions/group",
        headers={"X-Metabase-Session": session_id}
    ).json()


def get_users(url, session_id):
    return requests.get(
        f"{url}/user?status=active&limit=200&offset=0",
        headers={"X-Metabase-Session": session_id}
    ).json()["data"]


def get_memberships(url, session_id):
    data = requests.get(
        f"{url}/permissions/membership",
        headers={"X-Metabase-Session": session_id}
    ).json()

    memberships = []
    for _, items in data.items():
        for m in items:
            memberships.append(m)

    return memberships

def fetch_users(url, session_id):
    logger.info("Running FETCH USER DETAILS")

    groups = get_groups(url, session_id)
    users = get_users(url, session_id)

    group_map = {g["id"]: g["name"] for g in groups}

    data = []
    for u in users:
        data.append({
            "user_id": u["id"],
            "name": u["common_name"],
            "email": u["email"],
            "groups": [
                {"id": gid, "name": group_map.get(gid)}
                for gid in u.get("group_ids", [])
            ]
        })

    file_path = os.path.join(
        LOG_DIR,
        f"existing_user_maps_{datetime.now().strftime('%Y-%m-%d')}.json"
    )

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Snapshot saved → {file_path}")

def delete_groups(url, session_id):
    logger.info("Running DELETE GROUPS")

    groups = get_groups(url, session_id)
    memberships = get_memberships(url, session_id)

    protected = {1, 2}

    for g in groups:
        gid = g["id"]
        gname = g["name"]

        if gid in protected:
            logger.info(f"Skipping protected group: {gname}")
            continue

        for m in memberships:
            if m["group_id"] == gid:
                try:
                    requests.delete(
                        f"{url}/permissions/membership/{m['membership_id']}",
                        headers={"X-Metabase-Session": session_id}
                    )
                except:
                    logger.warning(f"Skipping membership delete: {m['membership_id']}")

        try:
            requests.delete(
                f"{url}/permissions/group/{gid}",
                headers={"X-Metabase-Session": session_id}
            )
            logger.info(f"Deleted group: {gname}")
        except Exception as e:
            logger.warning(f"Failed deleting group {gname}")

def remap_users(url, session_id):
    logger.info("Running REMAP USERS")

    files = [
        f for f in os.listdir(LOG_DIR)
        if f.startswith("existing_user_maps_")
    ]

    if not files:
        raise Exception("No extract file found")

    file_path = os.path.join(LOG_DIR, sorted(files)[-1])

    with open(file_path) as f:
        data = json.load(f)

    groups = get_groups(url, session_id)
    users = get_users(url, session_id)

    group_map = {g["name"]: g["id"] for g in groups}
    user_map = {u["email"]: u for u in users}

    memberships = get_memberships(url, session_id)

    for u in data:
        email = u["email"]
        user = user_map.get(email)

        existing_ids = [g["id"] for g in u["groups"]]
        existing_names = [g["name"] for g in u["groups"]]

        if not user:
            logger.warning(f"User not found: {email}")
            u["status"] = "USER_NOT_FOUND"
            continue

        user_id = user["id"]

        updated_ids = []
        updated_names = []

        for g_name in existing_names:
            if g_name in group_map:
                updated_ids.append(group_map[g_name])
                updated_names.append(g_name)
            else:
                logger.warning(f"Group not found: {g_name}")

        for m in memberships:
            if m["user_id"] != user_id:
                continue
            if m["group_id"] in [1, 2]:
                continue

            try:
                requests.delete(
                    f"{url}/permissions/membership/{m['membership_id']}",
                    headers={"X-Metabase-Session": session_id}
                )
            except:
                pass

        for gid in updated_ids:
            if gid == 1:
                continue

            try:
                requests.post(
                    f"{url}/permissions/membership",
                    headers={"X-Metabase-Session": session_id},
                    json={"user_id": user_id, "group_id": gid}
                )
            except:
                logger.warning(f"Skipping duplicate add for {email}")

        u["existing_groups_id"] = existing_ids
        u["existing_groups_name"] = existing_names
        u["updated_groups_id"] = updated_ids
        u["updated_groups_name"] = updated_names
        u["status"] = "SUCCESS"

        logger.info(f"Remapped user: {email}")

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Updated JSON file: {file_path}")
    logger.info("Remap completed")

def main():
    config = load_config()

    url = config["url"]
    username = config["username"]
    password = config["password"]

    session_id = get_session(url, username, password)

    logger.info(f"Execution Mode: {EXECUTION_MODE}")

    if EXECUTION_MODE == "fetch_user_details":
        fetch_users(url, session_id)

    elif EXECUTION_MODE == "delete_all_the_groups":
        delete_groups(url, session_id)

    elif EXECUTION_MODE == "remap_the_users":
        remap_users(url, session_id)


if __name__ == "__main__":
    main()