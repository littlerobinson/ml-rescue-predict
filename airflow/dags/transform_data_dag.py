import csv
import logging
from datetime import datetime, timedelta
import os
import time

import pandas as pd
import requests
from bs4 import BeautifulSoup

from s3_to_postgres import S3ToPostgresOperator

from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

# Globals variables
OPENWEATHERMAP_API = Variable.get("OPENWEATHERMAP_API")
S3_BUCKET_NAME = Variable.get("S3BucketName")
S3_PATH = Variable.get("S3Path")
DATASET_ACCIDENT_ID = "53698f4ca3a729239d2036df"
DATASET_ACCIDENT_URL = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ACCIDENT_ID}/"

dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}


def _download_file_from_s3(bucket_name, s3_key, local_dir):
    """
    Download a file from an S3 bucket to a local directory.

    :param bucket_name: The name of the S3 bucket.
    :param s3_key: The key of the file in the S3 bucket.
    :param local_dir: The local directory where the file will be saved.
    """
    # Initialize the S3 hook
    s3_hook = S3Hook(aws_conn_id="aws_default")

    # Check if the file exists in the S3 bucket
    if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
        # Ensure the local directory exists
        os.makedirs(local_dir, exist_ok=True)

        # Define the full local path including the filename
        local_path = os.path.join(local_dir, os.path.basename(s3_key))

        # Download the file
        try:
            s3_hook.get_key(key=s3_key, bucket_name=bucket_name).download_file(local_path)
            print(f"File downloaded successfully to {local_path}")
        except PermissionError as e:
            print(f"Permission error: {e}")
    else:
        print(f"File {s3_key} does not exist in the bucket {bucket_name}")


def _fetch_files(type="accidents"):
    logging.info("Starting to fetch files...")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    local_dir = "/tmp/s3_files"

    os.makedirs(local_dir, exist_ok=True)

    s3_keys = s3_hook.list_keys(
        bucket_name=S3_BUCKET_NAME, prefix=f"{S3_PATH}{type}/"
    )
    logging.info(f"S3 Keys: {s3_keys}")

    if not s3_keys:
        raise FileNotFoundError(
            f"No files found in {S3_PATH}{type}/ in {S3_BUCKET_NAME}."
        )

    for s3_key in s3_keys:
        file_name = os.path.basename(s3_key)
        local_path = os.path.join(local_dir, file_name)
        logging.info(f"Local_path: {local_path}")
        _download_file_from_s3(S3_BUCKET_NAME, s3_key, local_path)
        logging.info(f"Downloaded {s3_key} to {local_path}")


with DAG(
    "transform_raw_data",
    default_args=dag_default_args,
    description="Transform raw data o a data for training",
    schedule_interval="0 0 30 12 *",
    tags=["ELT", "transform", "OLTP"],
) as dag:
    logging.info("transform_raw_data")

    start = BashOperator(task_id="start", bash_command="echo 'Start!'")

    with TaskGroup(group_id="data_branch") as data_branch:
        accident_data = PythonOperator(
            task_id="accident_data",
            python_callable=_fetch_files,
            op_args=['accidents']
        )
        meta_data = PythonOperator(
            task_id="meta_data",
            python_callable=_fetch_files,
            op_args=['meta']
        )
        [accident_data, meta_data]

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    start >> [data_branch] >> end
