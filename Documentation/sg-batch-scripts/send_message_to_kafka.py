import pandas as pd
import json
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import os
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

EXCEL_FILE_PATH = config.get("EXCEL", "file_path")
CHAUPAL_SHEET = config.get("EXCEL", "chaupal_sheet")
MI_STORIES_SHEET = config.get("EXCEL", "mi_stories_sheet")


class ExcelToKafkaProcessor:

    def __init__(self, excel_file_path: str, kafka_bootstrap_servers: str):
        self.excel_file_path = excel_file_path
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.producer = None
        self._setup_logging()
        self._setup_kafka_producer()
    
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
    
    def _send_to_kafka(self, topic: str, message: Dict[str, Any], identifier: str = None) -> bool:
        try:
            future = self.producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)
            self.logger.info(
                f"Message sent to topic '{topic}' - "
                f"Partition: {record_metadata.partition}, "
                f"Offset: {record_metadata.offset}"
            )
            return True
        except KafkaError as e:
            self.logger.error(f"Failed to send message to topic '{topic}': {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Unexpected error sending to topic '{topic}': {str(e)}")
            return False
    
    def process_chaupal_data(self):
        try:
            self.logger.info("Starting to process Bihar Chaupal data...")
            
            df = pd.read_excel(
                self.excel_file_path,
                sheet_name=CHAUPAL_SHEET
            )
            
            self.logger.info(f"Read {len(df)} rows from Bihar Chaupal data sheet: {CHAUPAL_SHEET}")
            
            success_count = 0
            failure_count = 0
            
            for index, row in df.iterrows():
                try:
                    raw_challenges = str(row['Challenges']) if pd.notna(row['Challenges']) else ""
                    formatted_challenges = f"[{raw_challenges}]" if raw_challenges else ""

                    message = {
                        "userId": str(row['id']),
                        "challenges": formatted_challenges,
                        "district": str(row['District']) if pd.notna(row['District']) else "",
                        "state": "bihar" 
                    }
                    
                    if self._send_to_kafka(CHAUPAL_TOPIC, message, identifier=message["userId"]):
                        success_count += 1
                    else:
                        failure_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing row {index} in Chaupal data: {str(e)}")
                    failure_count += 1
            
            self.logger.info(
                f"Chaupal data processing completed. "
                f"Success: {success_count}, Failed: {failure_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process Chaupal data: {str(e)}")
            raise
    
    def process_mi_stories_data(self):
        try:
            self.logger.info("Starting to process Bihar MI stories data...")
            
            df = pd.read_excel(
                self.excel_file_path,
                sheet_name=MI_STORIES_SHEET
            )
            
            self.logger.info(f"Read {len(df)} rows from Bihar MI stories sheet: {MI_STORIES_SHEET}")
            
            success_count = 0
            failure_count = 0
            
            for index, row in df.iterrows():
                try:
                    message = {
                        "storyId": str(row['id']),
                        "actionSteps": str(row['action_steps']) if pd.notna(row['action_steps']) else "",
                        "impact": str(row['impact']) if pd.notna(row['impact']) else "",
                        "district": str(row['detected_district']) if pd.notna(row['detected_district']) else "",
                        "state": str(row['detected_state']) if pd.notna(row['detected_state']) else "",
                        "role": str(row['designation']) if pd.notna(row['designation']) else ""
                    }
                    
                    if self._send_to_kafka(MI_STORIES_TOPIC, message, identifier=message["storyId"]):
                        success_count += 1
                    else:
                        failure_count += 1
                        
                except Exception as e:
                    self.logger.error(f"Error processing row {index} in MI stories data: {str(e)}")
                    failure_count += 1
            
            self.logger.info(
                f"MI stories data processing completed. "
                f"Success: {success_count}, Failed: {failure_count}"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to process MI stories data: {str(e)}")
            raise
    
    def run(self):
        try:
            self.logger.info("="*80)
            self.logger.info("Starting Excel to Kafka processing pipeline")
            self.logger.info(f"Excel file: {self.excel_file_path}")
            self.logger.info("="*80)
            self.process_chaupal_data()
            self.process_mi_stories_data()
            self.producer.flush()
            self.logger.info("="*80) 
            self.logger.info("Processing pipeline completed successfully")
            self.logger.info("="*80)
            
        except Exception as e:
            self.logger.error(f"Pipeline execution failed: {str(e)}")
            raise
        finally:
            if self.producer:
                self.producer.close()
                self.logger.info("Kafka producer closed")


def main():
    processor = ExcelToKafkaProcessor(
        excel_file_path=EXCEL_FILE_PATH,
        kafka_bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    
    processor.run()


if __name__ == '__main__':
    main()
