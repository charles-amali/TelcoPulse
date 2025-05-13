import boto3
import pandas as pd
import json
import time
import logging
from botocore.exceptions import BotoCoreError, ClientError

# ------------------- Config -------------------
CSV_FILE_PATH = 'Data/mobile-logs.csv'
STREAM_NAME = 'telco-network-metrics'
REGION_NAME = 'eu-west-1'
SLEEP_TIME = 1  # Simulate 1 second delay

# ------------------- Logging Setup -------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("streaming.log"),
        logging.StreamHandler()
    ]
)

# ------------------- Kinesis Client -------------------
def get_kinesis_client(region_name):
    try:
        return boto3.client('kinesis', region_name=region_name)
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to create Kinesis client: {e}")
        raise

# ------------------- Read CSV -------------------
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        logging.info(f"CSV loaded successfully with {len(df)} rows.")
        return df
    except Exception as e:
        logging.error(f"Error reading CSV file: {e}")
        raise

# ------------------- Prepare Record -------------------
def prepare_record(row):
    try:
        return {
            "timestamp": row["hour"],
            "operator": row["operator"],
            "network": row["network"],
            "provider": row["provider"],
            "activity": row["activity"],
            "postal_code": str(row["postal_code"]),
            "signal": float(row["signal"]),
            "precision": float(row["precission"]),
            "status": row["status"]
        }
    except Exception as e:
        logging.warning(f"Failed to prepare record: {e}")
        return None

# ------------------- Send Record to Kinesis -------------------
def send_to_kinesis(client, record, stream_name, partition_key):
    try:
        response = client.put_record(
            StreamName=stream_name,
            Data=json.dumps(record),
            PartitionKey=partition_key
        )
        logging.info(f"Sent record: {record}")
        return response
    except (BotoCoreError, ClientError) as e:
        logging.error(f"Failed to send record to Kinesis: {e}")
        return None

# ------------------- Main Streaming Function -------------------
def stream_records():
    kinesis_client = get_kinesis_client(REGION_NAME)
    df = read_csv(CSV_FILE_PATH)

    for index, row in df.iterrows():
        record = prepare_record(row)
        if record is None:
            logging.warning(f"Skipping row {index + 1} due to record preparation failure.")
            continue

        partition_key = row.get("operator", "")
        if pd.isna(partition_key) or not isinstance(partition_key, str) or not partition_key.strip():
            logging.warning(f"Skipping row {index + 1}: Invalid PartitionKey -> {partition_key}")
            continue

        send_to_kinesis(kinesis_client, record, STREAM_NAME, partition_key)
        time.sleep(SLEEP_TIME)

# ------------------- Run Script -------------------
if __name__ == '__main__':
    try:
        stream_records()
    except Exception as e:
        logging.critical(f"Fatal error: {e}", exc_info=True)
