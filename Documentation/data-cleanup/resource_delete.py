import json
import os
import configparser
from kafka import KafkaConsumer
import psycopg2
import psycopg2.extras
from dashboard_resource_helper import MetabaseUtil, load_metabase_config
import logging
from logging.handlers import RotatingFileHandler

# ---------------------------------------------------
# Load config
# ---------------------------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "config.ini")
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

LOG_FILE = "resource_delete.log"

logger = logging.getLogger("resource_delete_logger")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(message)s",
    "%Y-%m-%d %H:%M:%S"
)
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setFormatter(formatter)

logger.addHandler(handler)
logger.addHandler(console)

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

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[BROKER],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

logger.info(f"Listening for delete events on topic: {TOPIC}")

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
    logger.info(f"Checking table: {table_name}")

    if table_exists(table_name):
        logger.info(f"Dropping table: {table_name}")
        db_execute(f'DROP TABLE IF EXISTS public."{table_name}" CASCADE;')

        db_execute(f"DELETE FROM {ENV}_solutions WHERE solution_id = %s", (sol_id,))
        db_execute(f"DELETE FROM {ENV}_dashboard_metadata WHERE entity_id = %s", (sol_id,))
    else:
        logger.info(f"{table_name} does not exist.")

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
        logger.info(f"No project rows for {solution_id}")
        return False, [] 

    logger.info(f"Found Improvement Project IDs: {project_ids}")
    for pid in project_ids:
        logger.info(f"Deleting tasks for project_id: {pid}")
        db_execute(f"DELETE FROM {ENV}_tasks WHERE project_id = %s", (pid,))

    db_execute(f"DELETE FROM {ENV}_projects WHERE solution_id = %s", (solution_id,))
    db_execute(f"DELETE FROM {ENV}_solutions WHERE solution_id = %s", (solution_id,))
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

for message in consumer:
    try:
        data = message.value
        
        if "type" not in data or "entityId" not in data:
            logger.error("Error: Invalid message. Skipping.")
            continue

        entity_type = data["type"]
        entity_id = data["entityId"]

        # ---------------------------------------------------
        # PROGRAM DELETE FLOW
        # ---------------------------------------------------
        if entity_type == "program":
            program_id = entity_id
            logger.info(f"started deleting the program: {program_id}")

            rows = db_query(
                f"SELECT DISTINCT solution_id FROM {ENV}_solutions WHERE program_id = %s",
                (program_id,)
            )
            solution_ids = [r["solution_id"] for r in rows]

            logger.info(f"Solution IDs under program: {solution_ids}")

            for sol in solution_ids:
                logger.info(f"\nProcessing solution: {sol}")

                ok, project_ids = process_improvement_project(sol)

                if not ok:
                    logger.info("No project → Checking survey & observation tables")
                    process_survey(sol)
                    process_observation(sol)

            logger.info(f"Deleting dashboard metadata for program_id: {program_id}")
            db_execute(
                f"DELETE FROM {ENV}_dashboard_metadata WHERE entity_id = %s",
                (program_id,)
            )
            logger.info(f"Successfully deleted dashboard metadata for program_id: {program_id}")
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
            logger.info(f"Processing single solution delete: {solution_id}")

            ok, project_ids = process_improvement_project(solution_id)

            if not ok:
                logger.info("No project → Checking survey & observation tables")
                process_survey(solution_id)
                process_observation(solution_id)

            collection_id = mb.get_collection_id(solution_id)
            mb.delete_collection(collection_id)
            list_of_groups = mb.get_permission_groups()
            group_id = mb.get_permission_group_id(list_of_groups,solution_id)
            mb.delete_permission_group(group_id)   

        else:
            logger.info("Skipping unsupported event type.")

    except Exception as e:
        logger.error("Error:", str(e))
