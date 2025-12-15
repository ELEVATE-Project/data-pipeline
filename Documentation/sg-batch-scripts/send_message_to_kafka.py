import pandas as pd
import json
import os
import boto3
import io
import re
import logging
import psycopg2
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
from typing import Dict, Any
from configparser import ConfigParser, ExtendedInterpolation

# -------------------------------
# Config Setup
# -------------------------------
base_dir = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(base_dir, "config.ini")
config = ConfigParser(interpolation=ExtendedInterpolation())
config.read(config_path)

# Extract config values
LOG_DIR = config.get("LOGS", "log_dir")
LOG_FILENAME_PREFIX = config.get("LOGS", "filename_prefix")

KAFKA_BOOTSTRAP_SERVERS = config.get("KAFKA", "bootstrap_servers")
CHAUPAL_TOPIC = config.get("KAFKA", "chaupal_topic")
MI_STORIES_TOPIC = config.get("KAFKA", "mi_stories_topic")

# AWS Config
AWS_BUCKET_NAME = config.get("AWS", "bucket_name")
AWS_REGION = config.get("AWS", "region_name")
AWS_ACCESS_KEY = config.get("AWS", "access_key_id")
AWS_SECRET_KEY = config.get("AWS", "secret_access_key")

class ExcelToKafkaProcessor:

    def __init__(self, kafka_bootstrap_servers: str, aws_config: Dict[str, str]):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.aws_config = aws_config
        self.producer = None
        self.s3_client = None
        self.db_conn = None
        self._setup_logging()
        self._setup_kafka_producer()
        self._setup_s3_client()
        self._setup_postgres_connection()
    
    def _setup_logging(self):
        os.makedirs(LOG_DIR, exist_ok=True)
        log_filename = os.path.join(LOG_DIR, f'{LOG_FILENAME_PREFIX}_{datetime.now().strftime("%Y%m%d")}.log')
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_filename),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized. Log file: {log_filename}")
    
    def _setup_kafka_producer(self):
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all', 
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            self.logger.info(f"Kafka producer initialized with bootstrap servers: {self.kafka_bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {str(e)}")
            raise

    def _setup_s3_client(self):
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=self.aws_config['region'],
                aws_access_key_id=self.aws_config['access_key'],
                aws_secret_access_key=self.aws_config['secret_key']
            )
            self.logger.info(f"S3 client initialized for bucket: {self.aws_config['bucket']}")
        except Exception as e:
            self.logger.error(f"Failed to initialize S3 client: {str(e)}")
            raise

    def _setup_postgres_connection(self):
        try:
            self.db_conn = psycopg2.connect(
                host=config.get("POSTGRES", "host"),
                port=config.get("POSTGRES", "port"),
                database=config.get("POSTGRES", "database"),
                user=config.get("POSTGRES", "user"),
                password=config.get("POSTGRES", "password")
            )
            self.db_conn.autocommit = True
            with self.db_conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS file_metadata (
                        id SERIAL PRIMARY KEY,
                        file_name TEXT UNIQUE NOT NULL,
                        total_no_of_rows INT,
                        total_no_of_messages_sent INT DEFAULT 0,
                        status TEXT,
                        error TEXT,
                        data_inserted_timestamp TIMESTAMP,
                        file_link TEXT
                    )
                """)
            self.logger.info("PostgreSQL connection established and table ensured.")
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def get_file_status(self, file_name):
        with self.db_conn.cursor() as cursor:
            cursor.execute("SELECT status FROM file_metadata WHERE file_name = %s", (file_name,))
            result = cursor.fetchone()
            return result[0] if result else None

    def upsert_file_metadata(self, file_name, total_rows=0, rows_sent=0, status="PROCESSING", error=None):
        timestamp = datetime.now()
        file_link = f"s3://{AWS_BUCKET_NAME}/{file_name}"
        with self.db_conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO file_metadata (file_name, total_no_of_rows, total_no_of_messages_sent, status, error, data_inserted_timestamp, file_link)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_name) 
                DO UPDATE SET 
                    total_no_of_rows = EXCLUDED.total_no_of_rows,
                    total_no_of_messages_sent = EXCLUDED.total_no_of_messages_sent,
                    status = EXCLUDED.status,
                    error = EXCLUDED.error,
                    data_inserted_timestamp = EXCLUDED.data_inserted_timestamp,
                    file_link = EXCLUDED.file_link
            """, (file_name, total_rows, rows_sent, status, error, timestamp, file_link))
            
    def _send_to_kafka(self, topic: str, message: Dict[str, Any], identifier: str = None) -> bool:
        try:
            future = self.producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)
            self.logger.info(
                f"Message sent - Partition: {record_metadata.partition}, Offset: {record_metadata.offset}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Failed to send message: {str(e)}")
            return False
    
    def process_data(self, df, state_name, topic, type_handler):
        success_count = 0
        failure_count = 0
        
        for index, row in df.iterrows():
            try:
                message = type_handler(row, state_name)
                if self._send_to_kafka(topic, message, identifier=message.get("id") or message.get("discussionId")):
                    success_count += 1
                else:
                    failure_count += 1
            except Exception as e:
                self.logger.error(f"Error processing row {index}: {str(e)}")
                failure_count += 1
        return success_count, failure_count

    def process_s3_files(self):
        bucket_name = self.aws_config['bucket']
        try:
            self.logger.info(f"Listing files in bucket: {bucket_name}")
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)

            if 'Contents' not in response:
                self.logger.info("No files found in the bucket.")
                return

            for obj in response['Contents']:
                file_key = obj['Key']
                if file_key.endswith('/'): continue
                
                filename = os.path.basename(file_key)
                
                status = self.get_file_status(filename)
                if status == "SUCCESS":
                    self.logger.info(f"Skipping {filename}, already processed successfully.")
                    continue
                
                # Determine type
                if '_chaupal.xlsx' in filename:
                    topic = CHAUPAL_TOPIC
                    handler = self._prepare_chaupal_message
                    raw_state_name = re.match(r'(.+)_chaupal\.xlsx', filename).group(1)
                elif '_mi_stories.xlsx' in filename:
                    topic = MI_STORIES_TOPIC
                    handler = self._prepare_mi_message
                    raw_state_name = re.match(r'(.+)_mi_stories\.xlsx', filename).group(1)
                else:
                    continue

                self.logger.info(f"Processing {filename} for state: {raw_state_name}")
                
                # Fetch content
                file_obj = self.s3_client.get_object(Bucket=bucket_name, Key=file_key)
                file_content = io.BytesIO(file_obj['Body'].read())
                df = pd.read_excel(file_content)
                total_rows = len(df)
                
                # Initial metadata entry
                self.upsert_file_metadata(filename, total_rows=total_rows, status="PROCESSING")
                
                success, failed = self.process_data(df, raw_state_name, topic, handler)
                
                final_status = "SUCCESS" if success == total_rows else "FAILED"
                error_msg = f"Processed {success}/{total_rows}" if final_status == "FAILED" else None
                
                self.upsert_file_metadata(filename, total_rows, success, final_status, error_msg)
                self.logger.info(f"Finished {filename}: {final_status} ({success}/{total_rows})")

        except Exception as e:
            self.logger.error(f"Error processing S3 files: {str(e)}")
            raise

    def _get_value(self, row, column):
        val = row.get(column)
        return str(val) if pd.notna(val) else ""

    def _prepare_chaupal_message(self, row, state):
        raw_challenges = self._get_value(row, 'Challenges')
        return {
            "id": self._get_value(row, 'id'),
            "Title": self._get_value(row, 'Title'),
            "User name": self._get_value(row, 'User name'),
            "User Location": self._get_value(row, 'User Location'),
            "Participant Count": self._get_value(row, 'Participant Count'),
            "Men": self._get_value(row, 'Men'),
            "Women": self._get_value(row, 'Women'),
            "Children": self._get_value(row, 'Children'),
            "challenges": f"[{raw_challenges}]" if raw_challenges else "",
            "Solutions": self._get_value(row, 'Solutions'),
            "author": self._get_value(row, 'author'),
            "Organization": self._get_value(row, 'Organization'),
            "language": self._get_value(row, 'language'),
            "Report Created At": self._get_value(row, 'Report Created At'),   
            "image_urls": self._get_value(row, 'image_urls'),
            "pdf_urls": self._get_value(row, 'pdf_urls'),
            "transcript_link": self._get_value(row, 'transcript_link'),
            "district": self._get_value(row, 'District'),
            "state": state,
            "Date of Discussion": self._get_value(row, 'Date of Discussion'),
            "created_at": str(datetime.now())
        }
    
    def _prepare_mi_message(self, row, state):
        return {
            "id": self._get_value(row, 'id'),
            "Title": self._get_value(row, 'Title'),
            "Report Created At": self._get_value(row, 'Report Created At'),
            "session": self._get_value(row, 'session'),
            "objective": self._get_value(row, 'objective'),
            "duration": self._get_value(row, 'duration'),
            "location": self._get_value(row, 'location'),
            "user_name": self._get_value(row, 'user_name'),
            "organization": self._get_value(row, 'organization'),
            "blurb": self._get_value(row, 'blurb'),
            "district": self._get_value(row, 'detected_district'),
            "designation": self._get_value(row, 'designation'),
            "action_steps": self._get_value(row, 'action_steps'),
            "content": self._get_value(row, 'content'),
            "Images": self._get_value(row, 'Images'),
            "Pdf": self._get_value(row, 'Pdf'),
            "detected_state": self._get_value(row, 'detected_state'),
            "impact": self._get_value(row, 'impact'),
            "transcript_link": self._get_value(row, 'transcript_link'),
            "state": state,
            "created_at": str(datetime.now())
        }

    def run(self):
        try:
            self.logger.info("="*80)
            self.logger.info("Starting Excel to Kafka processing pipeline")
            self.process_s3_files()
            if self.producer: self.producer.flush()
            if self.db_conn: self.db_conn.close()
            self.logger.info("Pipeline completed")
        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            raise
        finally:
            if self.producer: self.producer.close()

def main():
    aws_configuration = {
        'bucket': AWS_BUCKET_NAME,
        'region': AWS_REGION,
        'access_key': AWS_ACCESS_KEY,
        'secret_key': AWS_SECRET_KEY
    }

    processor = ExcelToKafkaProcessor(
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        aws_config=aws_configuration
    )
    
    processor.run()


if __name__ == '__main__':
    main()
