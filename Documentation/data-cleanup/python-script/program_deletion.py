import requests
import logging
import os
from datetime import datetime

# ----------------------------
# Setup Logging
# ----------------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_filename = os.path.join(LOG_DIR, f"program_fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Console handler (prints logs to terminal also)
console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
logging.getLogger().addHandler(console_handler)

logger = logging.getLogger(__name__)

import configparser

# ----------------------------
# Load Configuration
# ----------------------------
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_path)

program_deletion_config = config['ProgramDeletion']

# ----------------------------
# API Endpoints
# ----------------------------
LOGIN_URL = program_deletion_config['login_url']
DB_FIND_URL = program_deletion_config['db_find_url']

USERNAME = program_deletion_config['username']
PASSWORD = program_deletion_config['password']


def get_access_token():
    logger.info("Requesting access token from login API...")

    payload = {
        "identifier": USERNAME,
        "password": PASSWORD
    }
    headers = {
        "origin": program_deletion_config.get('origin', ''),
        "Content-Type": "application/json"
    }

    try:
        res = requests.post(LOGIN_URL, json=payload, headers=headers)
        res.raise_for_status()
        token = res.json().get("result", {}).get("access_token")

        if not token:
            logger.error("Access token not found in API response.")
            raise Exception("Access token missing.")

        logger.info("Successfully fetched access token.")
        return token

    except Exception as e:
        logger.exception("Failed to fetch access token.")
        raise e


def fetch_all_program_ids(access_token):
    logger.info("Fetching all program IDs using dbFind API...")

    headers = {
        "x-auth-token": access_token,
        "appname": "mentored",
        "Content-Type": "application/json"
    }

    payload = {
        "query": {"isAPrivateProgram":True},
        "sort": {"createdAt": "-1"},
        "projection": ["_id", "tenantId", "orgId"],
        "mongoIdKeys": ["_id"],
        "limit": 100000
    }

    try:
        res = requests.post(DB_FIND_URL, json=payload, headers=headers)
        res.raise_for_status()

        data = res.json().get("result", [])
        logger.info(f"Total programs fetched from API: {len(data)}")

        program_map = {}

        for item in data:
            pid = item.get("_id")
            t_id = item.get("tenantId")
            o_id = item.get("orgId")

            if pid:
                program_map[pid] = {
                    "tenant_id": t_id,
                    "org_id": o_id
                }

        logger.info(f"Total valid program_ids processed: {len(program_map)}")
        return program_map

    except Exception as e:
        logger.exception("Error occurred while fetching program data.")
        raise e


def delete_required_program_ids(all_program_ids, list_of_required_program_ids):
    logger.info(f"Deleting {len(list_of_required_program_ids)} program_ids from master list...")

    before = len(all_program_ids)

    for pid in list_of_required_program_ids:
        removed = all_program_ids.pop(pid, None)
        if removed:
            logger.info(f"Deleted program_id: {pid}")

    after = len(all_program_ids)
    logger.info(f"Programs before deletion: {before}, after deletion: {after}")

    return all_program_ids


if __name__ == "__main__":

    list_of_required_program_ids = program_deletion_config['list_of_required_program_ids'].split(',')

    logger.info("Starting program ID extraction script...")

    try:
        access_token = get_access_token()
        all_program_ids = fetch_all_program_ids(access_token)

        remaining_programs = delete_required_program_ids(
            all_program_ids,
            list_of_required_program_ids
        )
        print(remaining_programs)
        logger.info("Final program count after deletion: " + str(len(remaining_programs)))
        logger.info("Script executed successfully.")

    except Exception as e:
        logger.error("Script failed due to an error.")