import psycopg2
import psycopg2.extras
import json
import uuid
import datetime
import time
import logging
import sys
from kafka import KafkaProducer

import configparser
import os

# === Load Configuration ===
config = configparser.ConfigParser()
config_path = os.path.join(os.path.dirname(__file__), 'config.ini')
config.read(config_path)

# === PostgreSQL connection details ===
PGHOST = config['POSTGRES_DB']['HOST']
PGPORT = config['POSTGRES_DB']['PORT']
PGDBNAME = config['POSTGRES_DB']['DBNAME']
PGUSER = config['POSTGRES_DB']['USER']
PGPASSWORD = config['POSTGRES_DB']['PASSWORD']

# === Kafka Details ===
TABLE_NAME = config['POSTGRES_DB']['METADATA_TABLE']
KAFKA_BROKER = config['KAFKA']['BROKER']
TOPIC_1 = config['KAFKA']['TOPIC_PROJECT']
TOPIC_2 = config['KAFKA']['TOPIC_SURVEY']
TOPIC_3 = config['KAFKA']['TOPIC_OBSERVATION']

# === Logging Setup ===
log_filename = f"kafka_push_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

def get_db_connection():
    return psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDBNAME,
        user=PGUSER,
        password=PGPASSWORD
    )

def main():
    log("🚀 Script started to push kafka events")

    conn = None
    producer = None
    try:
        # Initialize Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
        conn = get_db_connection()
        # Use RealDictCursor to access columns by name
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # === Fetch and Process project-linked dashboard ===
        log("=== Fetch and Process project-linked dashboard ===")
        cursor.execute(f"""
            SELECT linked_to, entity_id
            FROM {TABLE_NAME}
            WHERE entity_type = 'solution'
              AND report_type = 'improvementProject'
              AND linked_to IS NOT NULL AND linked_to <> 'null'
              AND entity_id IS NOT NULL AND entity_id <> 'null';
        """)
        project_rows = cursor.fetchall()
        log(f"PROJECT QUERY RESULT COUNT = {len(project_rows)}")

        for row in project_rows:
            linked_to = row['linked_to']
            entity_id = row['entity_id']
            today = datetime.datetime.now().isoformat(timespec='seconds')
            random_id = f"{uuid.uuid4()}_{today}"

            project_json = {
                "_id": random_id,
                "reportType": "Project",
                "publishedAt": today,
                "dashboardData": {
                    "targetedProgram": linked_to,
                    "targetedSolution": entity_id
                }
            }

            try:
                producer.send(TOPIC_1, value=project_json)
                log(f"✅ Project event pushed to Kafka: {random_id}")
                log(f"🚀 Event pushed for solution: {entity_id} -> program: {linked_to}")
            except Exception as k_err:
                log(f"❌ Failed to push Project event to Kafka: {k_err}")
        
        
        log("=== Completed Processing project-linked dashboard ===")
        log("")

        # === Fetch and Process state-linked dashboards ===
        log("=== Fetch and Process state-linked dashboards ===")
        cursor.execute(f"""
            SELECT DISTINCT entity_id
            FROM {TABLE_NAME}
            WHERE entity_type = 'state'
              AND entity_id IS NOT NULL AND entity_id <> 'null';
        """)
        state_rows = cursor.fetchall()
        log(f"STATE QUERY RESULT COUNT = {len(state_rows)}")

        for row in state_rows:
            entity_id = row['entity_id']
            today = datetime.datetime.now().isoformat(timespec='seconds')
            random_id = f"{uuid.uuid4()}_{today}"

            state_json = {
                "_id": random_id,
                "reportType": "Project",
                "publishedAt": today,
                "dashboardData": {
                    "targetedState": entity_id
                }
            }

            try:
                producer.send(TOPIC_1, value=state_json)
                log(f"✅ State event pushed to Kafka: {random_id}")
                log(f"🚀 Event pushed for state: {entity_id}")
            except Exception as k_err:
                log(f"❌ Failed to push State event to Kafka: {k_err}")
            

        log("=== Completed Processing state-linked dashboards ===")
        log("")

        # === Fetch and Process district-linked dashboards ===
        log("=== Fetch and Process district-linked dashboards ===")
        cursor.execute(f"""
            SELECT DISTINCT entity_id
            FROM {TABLE_NAME}
            WHERE entity_type = 'district'
              AND entity_id IS NOT NULL AND entity_id <> 'null';
        """)
        district_rows = cursor.fetchall()
        log(f"DISTRICT QUERY RESULT COUNT = {len(district_rows)}")

        for row in district_rows:
            entity_id = row['entity_id']
            today = datetime.datetime.now().isoformat(timespec='seconds')
            random_id = f"{uuid.uuid4()}_{today}"

            district_json = {
                "_id": random_id,
                "reportType": "Project",
                "publishedAt": today,
                "dashboardData": {
                    "targetedDistrict": entity_id
                }
            }

            try:
                producer.send(TOPIC_1, value=district_json)
                log(f"✅ District event pushed to Kafka: {random_id}")
                log(f"🚀 Event pushed for district: {entity_id}")
            except Exception as k_err:
                log(f"❌ Failed to push District event to Kafka: {k_err}")
        

        log("=== Completed Processing district-linked dashboards ===")
        log("")

        # === Fetch and Process survey-linked dashboard ===
        log("=== Fetch and Process survey-linked dashboard ===")
        cursor.execute(f"""
            SELECT linked_to, entity_id
            FROM {TABLE_NAME}
            WHERE entity_type = 'solution'
              AND report_type = 'survey'
              AND linked_to IS NOT NULL AND linked_to <> 'null'
              AND entity_id IS NOT NULL AND entity_id <> 'null';
        """)
        survey_rows = cursor.fetchall()
        log(f"SURVEY QUERY RESULT COUNT = {len(survey_rows)}")

        for row in survey_rows:
            linked_to = row['linked_to']
            entity_id = row['entity_id']
            today = datetime.datetime.now().isoformat(timespec='seconds')
            random_id = f"{uuid.uuid4()}_{today}"

            survey_json = {
                "_id": random_id,
                "reportType": "Survey",
                "publishedAt": today,
                "dashboardData": {
                    "targetedProgram": linked_to,
                    "targetedSolution": entity_id
                }
            }

            try:
                producer.send(TOPIC_2, value=survey_json)
                log(f"✅ Survey event pushed to Kafka: {random_id}")
                log(f"🚀 Event pushed for solution: {entity_id} -> program: {linked_to}")
            except Exception as k_err:
                log(f"❌ Failed to push Survey event to Kafka: {k_err}")
            

        log("=== Completed Processing survey-linked dashboard ===")
        log("")

        # === Fetch and Process observation-linked dashboard ===
        log("=== Fetch and Process observation-linked dashboard ===")
        cursor.execute(f"""
            SELECT linked_to, entity_id, is_rubrics, parent_name
            FROM {TABLE_NAME}
            WHERE entity_type = 'solution'
              AND report_type = 'observation'
              AND linked_to IS NOT NULL AND linked_to <> 'null'
              AND entity_id IS NOT NULL AND entity_id <> 'null'
              AND parent_name IS NOT NULL AND parent_name <> 'null';
        """)
        observation_rows = cursor.fetchall()
        log(f"OBSERVATION QUERY RESULT COUNT = {len(observation_rows)}")

        for row in observation_rows:
            linked_to = row['linked_to']
            entity_id = row['entity_id']
            is_rubrics = row['is_rubrics']
            parent_name = row['parent_name']

            today = datetime.datetime.now().isoformat(timespec='seconds')
            random_id = f"{uuid.uuid4()}_{today}"

            # Logic for isRubric
            is_rubric_bool = True if is_rubrics == "t" or is_rubrics is True else False

            observation_json = {
                "_id": random_id,
                "reportType": "Observation",
                "publishedAt": today,
                "dashboardData": {
                    "targetedProgram": linked_to,
                    "targetedSolution": entity_id,
                    "isRubric": is_rubric_bool,
                    "entityType": parent_name
                }
            }

            try:
                producer.send(TOPIC_3, value=observation_json)
                log(f"✅ Observation event pushed to Kafka: {random_id}")
                log(f"🚀 Event pushed for solution: {entity_id} -> program: {linked_to}")
            except Exception as k_err:
                log(f"❌ Failed to push Observation event to Kafka: {k_err}")
            

        log("=== Completed Processing observation-linked dashboard ===")
        
        # Flush producer to ensure all messages are sent
        if producer:
            producer.flush()

        cursor.close()
        conn.close()

    except Exception as e:
        log(f"❌ Script Error: {e}")
        try:
            if conn: conn.close()
            if producer: producer.close()
        except:
            pass

    log("🏁 Script completed")

if __name__ == "__main__":
    main()
