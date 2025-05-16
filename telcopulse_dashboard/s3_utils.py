# s3_utils.py

import boto3
import pandas as pd
import s3fs
import os
from typing import List, Optional
from dotenv import load_dotenv

load_dotenv()

class S3Connector:
    def __init__(self):
        self.AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
        self.AWS_REGION = os.getenv('AWS_REGION')
        self.S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY,
            region_name=self.AWS_REGION
        )

        self.fs = s3fs.S3FileSystem(
            key=self.AWS_ACCESS_KEY_ID,
            secret=self.AWS_SECRET_ACCESS_KEY,
            client_kwargs={'region_name': self.AWS_REGION}
        )

    def list_parquet_files(self, prefix: str = '') -> List[str]:
        """Recursively list all .parquet files under the given prefix."""
        try:
            path = f"s3://{self.S3_BUCKET_NAME}/{prefix}"
            all_files = self.fs.find(path)
            parquet_files = [f for f in all_files if f.endswith(".parquet")]
            return parquet_files
        except Exception as e:
            print(f"Error listing parquet files: {e}")
            return []

    def read_parquet_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Read a single parquet file from S3 into a DataFrame."""
        try:
            with self.fs.open(file_path) as f:
                return pd.read_parquet(f)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return None

    def read_all_parquet(self, prefix: str = '') -> Optional[pd.DataFrame]:
        """
        Read all .parquet files under a prefix (recursively) into one DataFrame.
        """
        try:
            parquet_files = self.list_parquet_files(prefix)
            if not parquet_files:
                print("No parquet files found.")
                return None

            dfs = [self.read_parquet_file(file) for file in parquet_files if file]
            return pd.concat(dfs, ignore_index=True) if dfs else None
        except Exception as e:
            print(f"Failed to read all parquet files: {e}")
            return None
