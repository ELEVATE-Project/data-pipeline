import json
import configparser
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras
from clean_metabase_dashboard import MetabaseUtil, load_metabase_config

# ---------------------------------------------------
# Load config
# ---------------------------------------------------
config = configparser.ConfigParser()
config.read('config.ini')
DB_HOST = config.get('Database', 'host')
DB_USER = config.get('Database', 'user')
DB_PASS = config.get('Database', 'password')
DB_NAME = config.get('Database', 'dbname')
ENV = config.get('Database', 'env')
TOPIC = config.get('Database', 'topic')
GROUP_ID = config.get('Database', 'group_id')
BROKER = config.get('Database', 'broker')
url, user, pwd = load_metabase_config()
mb = MetabaseUtil(url, user, pwd)

# ---------------------------------------------------
# Shared DB connection (persistent)
# ---------------------------------------------------
conn = psycopg2.connect(
    host=DB_HOST,
    user=DB_USER,
    password=DB_PASS,
    dbname=DB_NAME
)
conn.autocommit = True

def db_query(sql, params=None):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params or ())
        return cur.fetchall()

def db_execute(sql, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params or ())

# ---------------------------------------------------
# Kafka consumer
# ---------------------------------------------------
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Listening for delete events on topic: {TOPIC}")

def table_exists(table_name):
    rows = db_query("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema='public'
              AND table_name = %s
        );
    """, (table_name,))
    return rows[0]["exists"]


def drop_if_exists(table_name, sol_id):
    """Drop table if exists and delete solution + metadata."""
    print(f"Checking table: {table_name}")

    if table_exists(table_name):
        print(f"Dropping table: {table_name}")
        db_execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')

        db_execute(f"DELETE FROM {ENV}_solutions WHERE solution_id = %s", (sol_id,))
        db_execute(f"DELETE FROM {ENV}_dashboard_metadata WHERE entity_id = %s", (sol_id,))
    else:
        print(f"{table_name} does not exist.")

# ---------------------------------------------------
# Improvement Project Cleanup
# ---------------------------------------------------

def process_improvement_project(solution_id):
    project_rows = db_query(
        f"SELECT project_id FROM {ENV}_projects WHERE solution_id = %s",
        (solution_id,)
    )
    project_ids = [row["project_id"] for row in project_rows]

    if not project_ids:
        print(f"No project rows for {solution_id}")
        return False, []  # return flags for fallback

    print(f"Found Improvement Project IDs: {project_ids}")

    # Delete tasks first
    for pid in project_ids:
        print(f"Deleting tasks for project_id: {pid}")
        db_execute(f"DELETE FROM {ENV}_tasks WHERE project_id = %s", (pid,))

    # Delete project rows
    db_execute(f"DELETE FROM {ENV}_projects WHERE solution_id = %s", (solution_id,))

    # Delete solution
    db_execute(f"DELETE FROM {ENV}_solutions WHERE solution_id = %s", (solution_id,))

    # Delete metadata
    db_execute(f"DELETE FROM {ENV}_dashboard_metadata WHERE entity_id = %s", (solution_id,))

    return True, project_ids

# ---------------------------------------------------
# Observation & Survey Cleanup
# ---------------------------------------------------
def process_observation(solution_id):
    tables = [
        f"{solution_id}_domain",
        f"{solution_id}_status",
        f"{solution_id}_questions",
    ]
    for tbl in tables:
        drop_if_exists(tbl, solution_id)


def process_survey(solution_id):
    tables = [
        f"{solution_id}_survey_status",
        f"{solution_id}",
    ]
    for tbl in tables:
        drop_if_exists(tbl, solution_id)

# ---------------------------------------------------
# MAIN Kafka Loop
# ---------------------------------------------------
for message in consumer:
    try:
        data = message.value
        print("\nReceived message:", data)

        if "type" not in data or "entityId" not in data:
            print("Invalid message. Skipping.")
            continue

        entity_type = data["type"]
        entity_id = data["entityId"]

        # ---------------------------------------------------
        # PROGRAM DELETE FLOW
        # ---------------------------------------------------
        if entity_type == "program":
            program_id = entity_id
            print(f"Processing program delete: {program_id}")

            rows = db_query(
                f"SELECT DISTINCT solution_id FROM {ENV}_solutions WHERE program_id = %s",
                (program_id,)
            )
            solution_ids = [r["solution_id"] for r in rows]

            print("Solution IDs under program:", solution_ids)

            for sol in solution_ids:
                print(f"\nProcessing solution: {sol}")

                ok, project_ids = process_improvement_project(sol)

                if not ok:
                    print("No project → Checking survey & observation tables")
                    process_survey(sol)
                    process_observation(sol)

            print(f"Deleting dashboard metadata for program_id: {program_id}")
            db_execute(
                f"DELETE FROM {ENV}_dashboard_metadata WHERE entity_id = %s",
                (program_id,)
            )
            print(f"Deleted dashboard metadata for program_id: {program_id}")
            collection_id = mb.get_collection_id(program_id)
            mb.delete_collection(collection_id)
            list_of_groups = mb.get_permission_groups()
            group_id = mb.get_permission_group_id(list_of_groups,program_id)
            mb.delete_permission_group(group_id)
        # ---------------------------------------------------
        # SOLUTION DELETE FLOW
        # ---------------------------------------------------
        elif entity_type == "solution":
            solution_id = entity_id
            print(f"Processing single solution delete: {solution_id}")

            ok, project_ids = process_improvement_project(solution_id)

            if not ok:
                print("No project → Checking survey & observation tables")
                process_survey(solution_id)
                process_observation(solution_id)

            collection_id = mb.get_collection_id(solution_id)
            mb.delete_collection(collection_id)
            list_of_groups = mb.get_permission_groups()
            group_id = mb.get_permission_group_id(list_of_groups,solution_id)
            mb.delete_permission_group(group_id)   

        else:
            print("Skipping unsupported event type.")

    except Exception as e:
        print("Error:", str(e))
