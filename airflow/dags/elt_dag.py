from datetime import datetime, timedelta
import logging

import pandas as pd
import requests
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


def _fetch_accident_data():
    """
    Fetch accident data from a dataset URL and upload it to an S3 bucket.

    This function logs the start and end of the data fetching process,
    sends a GET request to the dataset URL to retrieve the data,
    checks if the request was successful, iterates over each resource
    in the dataset to process the files, extracts the file title and URL
    from each resource, checks if the file title contains specific keywords
    and does not contain certain phrases, formats the filename to replace
    spaces with underscores and ensures it ends with .csv, downloads the
    file content and writes it to a local file, uses the S3 hook to upload
    the file to an S3 bucket. It also logs any errors that occur during file downloads.
    """
    logging.info("fetch accident data started")

    response = requests.get(DATASET_ACCIDENT_URL)

    if response.status_code == 200:
        data_ressources = response.json()
        logging.info("Start downloading")

        for resource in data_ressources.get("resources", []):
            file_title = resource.get("title", "noname").lower()
            file_url = resource.get("url", "")
            file_format = resource.get("format", "")

            if (
                any(keyword in file_title for keyword in ["usa", "vehi", "lieu", "car"])
                and "vehicules-immatricules-baac" not in file_title
                and "description" not in file_title
                and file_format == "csv"
            ):
                filename = file_title.replace(" ", "_")
                if not filename.endswith(".csv"):
                    filename += ".csv"
                full_path_to_file = f"/tmp/{filename}"

                file_response = requests.get(file_url)

                if file_response.status_code == 200:
                    with open(full_path_to_file, "wb") as f:
                        f.write(file_response.content)

                    s3_hook = S3Hook(aws_conn_id="aws_s3")
                    s3_hook.load_file(
                        filename=full_path_to_file,
                        key=f"{S3_PATH}{filename}",
                        bucket_name=S3_BUCKET_NAME,
                        replace=True,
                    )
                    logging.info(f"Saved accidents data to S3 with name {filename}")
                else:
                    logging.error(f"Failed to download {filename}")

    logging.info("fetch accident data finished")


with DAG(
    "download_and_upload_raw_data_to_s3",
    default_args=dag_default_args,
    description="Download a CSV file and load it into an S3 bucket",
    schedule_interval="0 0 30 12 *",  # At 00:00 on day-of-month 30 in December.
) as dag:
    logging.info("download_and_upload_raw_data_to_s3")
    with TaskGroup(group_id="accident_branch") as accident_branch:
        start = BashOperator(task_id="start", bash_command="echo 'Start!'")
        fetch_weather_data = PythonOperator(
            task_id="fetch_accident_data",
            python_callable=_fetch_accident_data,
            retries=3,
            retry_delay=timedelta(minutes=10),
        )
