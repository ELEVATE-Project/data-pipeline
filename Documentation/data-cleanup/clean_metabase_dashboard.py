import requests
import json
import configparser


def load_metabase_config():
    config = configparser.ConfigParser()
    config.read("config.ini")

    url = config.get("Metabase", "url")
    username = config.get("Metabase", "username")
    password = config.get("Metabase", "password")

    return url, username, password


class MetabaseUtil:

    def __init__(self, metabase_url, username, password):
        self.metabase_url = metabase_url.rstrip("/")
        self.username = username
        self.password = password
        self.session_token = None

    # ---------------------------------------------------------
    # Get or refresh token
    # ---------------------------------------------------------
    def get_session_token(self):
        if self.session_token:
            return self.session_token

        url = f"{self.metabase_url}/session"
        payload = {"username": self.username, "password": self.password}

        resp = requests.post(url, json=payload)
        if resp.status_code == 200:
            token = resp.json().get("id")
            self.session_token = token
            return token

        raise Exception(f"Authentication failed: {resp.status_code} {resp.text}")

    # ---------------------------------------------------------
    # GET /api/dashboard
    # ---------------------------------------------------------
    def list_collections(self):
        url = f"{self.metabase_url}/collection"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.get_session_token(),
        }

        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"[ERROR] Failed to delete collection ({resp.status_code}): {resp.text}")
            return False

    # ---------------------------------------------------------
    # Validate + return dashboard ID
    # ---------------------------------------------------------
    def get_collection_id(self, target_id):
        coll_id = []
        collections = self.list_collections()
        for col in collections:
            if target_id in (col.get("description") or ""):
                coll_id.append(col.get("id"))
        if len(coll_id) > 0:
            print(f"collection IDs to delete: {coll_id}")
            return coll_id
        else:
            return []
    # ---------------------------------------------------------
    # Delete a Collection (Dashboard Folder)
    # ---------------------------------------------------------
    def delete_collection(self, collection_id):
        print(f">>> Deleting Metabase collection ID: {collection_id}")
        
        if len(collection_id) == 0:
            print(">>> Collection ID is None, skipping deletion.")
            return False
        else:
            for cid in collection_id:
                url = f"{self.metabase_url}/collection/{cid}"
                print(f">>> PUT URL: {url}")

                headers = {
                    "Content-Type": "application/json",
                    "X-Metabase-Session": self.get_session_token(),
                }

                payload = {
                    "archived": True
                }
                resp = requests.put(url, headers=headers, json=payload)
                if resp.status_code == 200:
                    print(f">>> Successfully archived collection ID: {cid}")
                else:
                    print(f"[ERROR] Failed to archive collection ({resp.status_code}): {resp.text}")

    # ---------------------------------------------------------
    # Get All Permission Groups
    # ---------------------------------------------------------
    def get_permission_groups(self):
        url = f"{self.metabase_url}/permissions/group"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.get_session_token(),
        }

        resp = requests.get(url, headers=headers)

        if resp.status_code == 200:
            return resp.json()

        raise Exception(
            f"Failed to fetch permission groups ({resp.status_code}): {resp.text}"
        )

    # ---------------------------------------------------------
    # Delete Permission Group
    # ---------------------------------------------------------
    def delete_permission_group(self, group_id: int):
        url = f"{self.metabase_url}/permissions/group/{group_id}"
        headers = {
            "Content-Type": "application/json",
            "X-Metabase-Session": self.get_session_token(),
        }

        resp = requests.delete(url, headers=headers)

        if resp.status_code == 204:
            print(f"Group {group_id} deleted successfully.")
            return True

        print(
            f"[ERROR] Failed to delete group {group_id}: "
            f"{resp.status_code}, {resp.text}"
        )
        return False

    # ---------------------------------------------------------
    # Find Group ID for given Program
    # ---------------------------------------------------------
    @staticmethod
    def get_permission_group_id(groups, program_id):
        for group in groups:
            if program_id in group.get("name", ""):
                return group["id"]
        return None
