import boto3
import pandas as pd
import json
import time

# Config
CSV_FILE_PATH = 'Data/mobile-logs.csv'        
STREAM_NAME = 'telco-network-metrics'         
REGION_NAME = 'eu-west-1'                     
SLEEP_TIME = 1                                
# Initialize Kinesis client
kinesis_client = boto3.client('kinesis', region_name=REGION_NAME)

# Read CSV file
df = pd.read_csv(CSV_FILE_PATH)

# Loop through rows and stream selected fields
for index, row in df.iterrows():
    record = {
        
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

    json_record = json.dumps(record) 

    partition_key = row["operator"]  # Use the operator as partition key
    if pd.isna(partition_key) or not isinstance(partition_key, str) or not partition_key.strip():
        print(f"Skipping record {index + 1}: Invalid PartitionKey -> {partition_key}")
        continue
    
    # Send to Kinesis stream
    response = kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json_record,
        PartitionKey=partition_key,
    )

    print(f"Sent record {index + 1}: {json_record}")
    time.sleep(SLEEP_TIME)
