import psycopg2
import psycopg2.extras
from psycopg2 import sql
import json
import logging
import datetime
import requests
import sys
import uuid
from kafka import KafkaProducer

import configparser
import os

# === Load Configuration ===
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_path)

# === Logging Setup ===
log_filename = f"re_push_user_kafka_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
# Also log to stdout
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logging.getLogger().addHandler(console_handler)

def log(message):
    logging.info(message)

# === PostgreSQL connection details ===
PGHOST = config['POSTGRES_DB']['HOST']
PGPORT = config['POSTGRES_DB']['PORT']
PGDBNAME = config['POSTGRES_DB']['USERS_DBNAME']
PGUSER = config['POSTGRES_DB']['USER']
PGPASSWORD = config['POSTGRES_DB']['PASSWORD']
USERS_TABLE = config['POSTGRES_DB']['USERS_TABLE']

# === API connection details ===
ENTITY_API = config['API']['ENTITY_API']
INTERNAL_TOKEN = config['API']['INTERNAL_TOKEN']

# === Kafka connection details ===
KAFKA_BROKER = config['KAFKA']['BROKER']
TOPIC = config['KAFKA']['TOPIC_USER']

def get_db_connection():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDBNAME,
        user=PGUSER,
        password=PGPASSWORD
    )

def main():
    conn = None
    producer = None
    session = None
    try:
        # Initialize Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        # Initialize Requests Session for connection pooling
        session = requests.Session()
        session.headers.update({
            "content-type": "application/json",
            "internal-access-token": INTERNAL_TOKEN
        })

        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # === Metadata Stats ===
        log(f"📊 Fetching user metadata from '{USERS_TABLE}'...")

        cursor.execute(sql.SQL("SELECT COUNT(*) as count FROM {};").format(sql.Identifier(USERS_TABLE)))
        total_rows = cursor.fetchone()['count']

        cursor.execute(sql.SQL("SELECT COUNT(*) as count FROM {} WHERE status = 'ACTIVE';").format(sql.Identifier(USERS_TABLE)))
        active_users = cursor.fetchone()['count']

        cursor.execute(sql.SQL("SELECT COUNT(*) as count FROM {} WHERE deleted_at IS NOT NULL;").format(sql.Identifier(USERS_TABLE)))
        deleted_users = cursor.fetchone()['count']

        log("------------------------------------")
        log(f"Total Users       : {total_rows}")
        log(f"Active Users      : {active_users}")
        log(f"Deleted Users     : {deleted_users}")
        log("------------------------------------")

        # === Fetch and Print Enriched User Info ===
        log("📦 Generating enriched user JSONs:")
        log("------------------------------------")

        # Fetch main user data
        query = sql.SQL("""
            SELECT id, name, username, tenant_code, created_at, updated_at, status, meta 
            FROM {} 
            ORDER BY created_at;
        """).format(sql.Identifier(USERS_TABLE))
        cursor.execute(query)
        
        # We need a separate cursor for the nested loop logic
        # Using RealDictCursor for inner loop as well
        inner_cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        rows = cursor.fetchall() # Fetch all to iterate
        
        for row in rows:
            user_id = row['id']
            name = row['name']
            username = row['username']
            tenant = row['tenant_code']
            created_at = row['created_at']
            updated_at = row['updated_at']
            status = row['status']
            meta_json = row['meta']
            
            if isinstance(meta_json, str):
                try:
                    meta_json = json.loads(meta_json)
                except:
                    log(f"⚠️  [User ID: {user_id}] Invalid JSON string in meta field — Skipping")
                    continue
            
            print("\n\n\n") 
            log(f"🔎 Processing User ID: {user_id} | Name: {name}")

            # ## ORG LOGIC (merged orgs + roles query)
            org_query = """
                SELECT json_build_object(
                'created_by', COALESCE(inv.created_by, uo.user_id),
                'organizations', (
                    SELECT json_agg(org_with_roles)
                    FROM (
                    SELECT
                        o.id, o.name, o.code, o.description, o.status,
                        o.related_orgs, o.tenant_code, o.meta, o.created_by, o.updated_by,
                        (
                        SELECT json_agg(ur)
                        FROM (
                            SELECT
                            ur.id, ur.title, ur.label, ur.user_type, ur.status,
                            ur.organization_id, ur.visibility, ur.tenant_code, ur.translations
                            FROM user_organization_roles uor
                            JOIN user_roles ur
                            ON uor.role_id = ur.id AND uor.tenant_code = ur.tenant_code
                            WHERE uor.user_id = uo.user_id
                            AND uor.organization_code = o.code
                            AND uor.tenant_code = o.tenant_code
                            AND uor.deleted_at IS NULL
                        ) ur
                        ) AS roles
                    FROM user_organizations uo
                    JOIN organizations o
                        ON o.code = uo.organization_code AND o.tenant_code = uo.tenant_code
                    WHERE uo.user_id = %s
                    ) org_with_roles
                )
                ) as merged
                FROM user_organizations uo
                LEFT JOIN organization_user_invites oui ON oui.username = %s
                LEFT JOIN invitations inv ON inv.id = oui.invitation_id
                WHERE uo.user_id = %s
                LIMIT 1;
            """
            inner_cursor.execute(org_query, (user_id, username, user_id))
            merged_orgs_row = inner_cursor.fetchone()
            merged_orgs = merged_orgs_row['merged'] if merged_orgs_row else {}


            deleted = "true" if status == "DELETED" else "false"

            # Check for empty meta
            if not meta_json:
                log(f"⚠️  [User ID: {user_id}] Empty meta object — Skipping")
                continue
            
            # Extract unique entity IDs from meta values
            entity_ids = set()
            try:
                for key, value in meta_json.items():
                    if isinstance(value, list):
                        for item in value:
                            if item: entity_ids.add(str(item))
                    elif value:
                        entity_ids.add(str(value))
            except Exception as e:
                log(f"⚠️  [User ID: {user_id}] Error parsing meta values: {e} — Skipping")
                continue

            if not entity_ids:
                log(f"⚠️  [User ID: {user_id}] No entity IDs found in meta — Skipping")
                continue
            
            # Prepare API Payload
            id_list = list(entity_ids)
            payload = {
                "query": {
                    "_id": { "$in": id_list },
                    "tenantId": tenant
                },
                "mongoIdKeys": "_id",
                "projection": ["_id", "metaInformation.name", "metaInformation.externalId"]
            }

            log(f"📡 [User ID: {user_id}] Fetching entity metadata for: {','.join(id_list)}")

            # API Call
            try:
                response = session.post(
                    ENTITY_API,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code != 200:
                    log(f"🚫 [User ID: {user_id}] API response not 200 (Got {response.status_code}) — Skipping")
                    continue
                
                body = response.json()
                
                if "ENTITY_NOT_FOUND" in str(body):
                     log(f"🚫 [User ID: {user_id}] ENTITY_NOT_FOUND in API — Skipping")
                     continue
                
                # enriched_fields construction
                enriched_fields = {}
                results = body.get("result", [])
                if results:
                    for res in results:
                        _id = res.get("_id")
                        meta_info = res.get("metaInformation", {})
                        if _id:
                            enriched_fields[_id] = {
                                "id": _id,
                                "name": meta_info.get("name"),
                                "externalId": meta_info.get("externalId")
                            }
                
                if not enriched_fields:
                     log(f"⚠️  [User ID: {user_id}] Failed to parse entity metadata — Skipping")
                     continue

                # Final JSON Construction
                expected_keys = ["block", "cluster", "district", "school", "state", "professional_role", "professional_subroles"]
                final_json_part = {}
                
                for key in expected_keys:
                    val = meta_json.get(key)
                    if isinstance(val, list):
                        mapped = []
                        for v in val:
                            if v and str(v) in enriched_fields:
                                mapped.append(enriched_fields[str(v)])
                        final_json_part[key] = mapped if mapped else None
                    elif val and str(val) in enriched_fields:
                         final_json_part[key] = enriched_fields[str(val)]
                    else:
                        final_json_part[key] = None

                log(f"✅  [User ID: {user_id}] Outputting enriched user JSON")

                created_by_val = merged_orgs.get("created_by") if merged_orgs else None
                organizations_val = merged_orgs.get("organizations") if merged_orgs else None

                def fmt_date(d):
                    return d.isoformat() if d else None
                
                user_json = {
                    "entity": "user",
                    "eventType": "create",
                    "entityId": int(user_id) if user_id else None,
                    "name": name,
                    "username": username,
                    "tenant_code": tenant,
                    "created_at": fmt_date(created_at),
                    "updated_at": fmt_date(updated_at),
                    "status": status,
                    "deleted": deleted,
                    "id": int(user_id) if user_id else None,
                    "created_by": int(created_by_val) if created_by_val else None,
                    "organizations": organizations_val
                }
                user_json.update(final_json_part)

                print(json.dumps(user_json))
                
                # Push to Kafka using Producer
                producer.send(TOPIC, value=user_json)
                log(f"📤  [User ID: {user_id}] JSON pushed to Kafka topic '{TOPIC}'")

            except Exception as e:
                log(f"❌ Error processing user {user_id}: {e}")
                continue
        
        if producer:
            producer.flush()

    except Exception as e:
        log(f"❌ Script Error: {e}")
    finally:
        if session:
            session.close()
        if producer:
            try:
                producer.close()
            except Exception:
                pass
        if conn:
            try:
                conn.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
