import requests
import json
import os
import sys
from datetime import datetime
from pyhocon import ConfigFactory
import logging

def setup_logger():
    root_dir = os.environ.get(
        "DATA_PIPELINE_ROOT",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../"))
    )
    LOG_DIR = os.path.join(root_dir, "logs")
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
        os.path.abspath(os.path.join(base_dir, "../../../..", "unified-common.conf"))
    )

    # Verify the file exists before parsing
    if not os.path.exists(unified_conf):
        print(f"Error: Unified configuration file not found at '{unified_conf}'.", file=sys.stderr)
        print("Please set the UNIFIED_PIPELINE_CONF environment variable or ensure 'unified-common.conf' exists in the root directory.", file=sys.stderr)
        sys.exit(1)

    try:
        config = ConfigFactory.parse_file(unified_conf)
        return {
            "url": config.get_string("metabase.url"),
            "username": config.get_string("metabase.username"),
            "password": config.get_string("metabase.password")
        }
    except Exception as e:
        print(f"Error parsing configuration at '{unified_conf}': {e}", file=sys.stderr)
        sys.exit(1)

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
    logger.info("Fetching users with pagination")

    all_users = []
    limit = 200
    offset = 0

    while True:
        res = requests.get(
            f"{url}/user",
            params={
                "status": "active",
                "limit": limit,
                "offset": offset
            },
            headers={"X-Metabase-Session": session_id}
        )

        res.raise_for_status()
        data = res.json()

        users_page = data.get("data", [])
        total = data.get("total", 0)

        all_users.extend(users_page)

        logger.info(f"Fetched {len(users_page)} users (offset={offset})")

        # Stop conditions
        if not users_page:
            break

        offset += limit

        if offset >= total:
            break

    logger.info(f"Total users fetched: {len(all_users)}")

    return all_users


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

    protected = {1, 2}

    for g in groups:
        gid = g["id"]
        gname = g["name"]

        if gid in protected:
            logger.info(f"Skipping protected group: {gname}")
            continue

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
            if gid in {1, 2}:
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

    if len(sys.argv) != 2:
            print(
                "Usage: python fetching_and_auto_mapping_groups.py "
                "[fetch_user_details | delete_all_the_groups | remap_the_users]"
            )
            sys.exit(1)

    execution_mode = sys.argv[1]

    valid_modes = {
        "fetch_user_details",
        "delete_all_the_groups",
        "remap_the_users"
    }

    if execution_mode not in valid_modes:
        print(f"Invalid execution mode: {execution_mode}")
        sys.exit(1)

    config = load_config()

    url = config["url"]
    username = config["username"]
    password = config["password"]

    session_id = get_session(url, username, password)

    logger.info(f"Execution Mode: {execution_mode}")

    if execution_mode == "fetch_user_details":
        fetch_users(url, session_id)

    elif execution_mode == "delete_all_the_groups":
        delete_groups(url, session_id)

    elif execution_mode == "remap_the_users":
        remap_users(url, session_id)


if __name__ == "__main__":
    main()