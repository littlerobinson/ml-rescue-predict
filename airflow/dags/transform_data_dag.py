import csv
import logging
from datetime import datetime, timedelta
import os
import time
import re
from collections import defaultdict
from itertools import product

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

    # Ensure the local directory exists
    os.makedirs(local_dir, exist_ok=True)

    # Define the full local path including the filename
    local_path = os.path.join(local_dir, os.path.basename(s3_key))

    # Check if the file already exists locally
    if os.path.exists(local_path):
        print(f"File already exists locally at {local_path}. Skipping download.")
        return

    # Check if the file exists in the S3 bucket
    if s3_hook.check_for_key(key=s3_key, bucket_name=bucket_name):
        # Download the file
        try:
            s3_hook.get_key(key=s3_key, bucket_name=bucket_name).download_file(
                local_path
            )
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

    s3_keys = s3_hook.list_keys(bucket_name=S3_BUCKET_NAME, prefix=f"{S3_PATH}{type}/")
    logging.info(f"S3 Keys: {s3_keys}")

    if not s3_keys:
        raise FileNotFoundError(
            f"No files found in {S3_PATH}{type}/ in {S3_BUCKET_NAME}."
        )

    for s3_key in s3_keys:
        logging.info(f"Local_path: {local_dir}")
        _download_file_from_s3(S3_BUCKET_NAME, s3_key, local_dir)
        logging.info(f"Downloaded {s3_key} to {local_dir}")


def _group_accident_data(ti, nb_years=2):
    logging.info(f"Starting group accident files for {nb_years} years...")
    local_dir = "/tmp/s3_files"
    pattern = re.compile(r"car.*(\d{4})")
    files_by_year = defaultdict(list)

    files = []

    # Check if directory exists
    if not os.path.exists(local_dir):
        logging.error(f"Directory {local_dir} does not exist.")
        return files

    # Iterate over files in the directory
    for file in os.listdir(local_dir):
        match = pattern.search(file)
        if match:
            year = int(match.group(1))
            files_by_year[year].append(file)

    # Find the two most recent files
    if files_by_year:
        sorted_years = sorted(files_by_year.keys(), reverse=True)
        recent_files = []

        for year in sorted_years:
            recent_files.extend(files_by_year[year])
            if len(recent_files) >= nb_years:
                break

        files = recent_files[:nb_years]
        logging.info(f"Files list --> {files}")
        # Read and concatenate CSV files
        dataframes = []
        for file in files:
            file_path = os.path.join(local_dir, file)
            logging.info(f"Processing file: {file_path}")

            # Check if the path is a file
            if os.path.isfile(file_path):
                df = pd.read_csv(file_path, sep=";", dtype=str)
                dataframes.append(df)

        if dataframes:
            concatenated_df = pd.concat(dataframes, ignore_index=True)
            # Create a new 'id' column based on 'Accident_Id' or 'Num_Acc'
            concatenated_df["id"] = concatenated_df["Accident_Id"].combine_first(
                concatenated_df["Num_Acc"]
            )
            # Drop the original 'Accident_Id' and 'Num_Acc' columns
            concatenated_df.drop(columns=["Accident_Id", "Num_Acc"], inplace=True)

            df_grouped = (
                concatenated_df.groupby(["jour", "mois", "an", "dep", "com"])
                .size()
                .reset_index(name="nombre_d_accidents")
            )

            # Save the concatenated DataFrame to a shared location or pass it using XCom
            output_path = "/tmp/s3_files/concatenated_accidents.csv"
            df_grouped.to_csv(output_path, index=False)

            logging.info(f"Concatenated CSV saved to {output_path}")
            # Push the output path to XCom for other tasks to use
            ti.xcom_push(key="concatenated_accidents_csv_path", value=output_path)
            logging.info(
                f"XCom push with key: 'concatenated_accidents_csv_path' and value: {output_path}"
            )

        else:
            logging.warning("No data to concatenate.")
    else:
        logging.warning("No files matched the pattern.")


def _group_data(ti):
    logging.info("Enrich data...")
    path_communes = "/tmp/s3_files/all_communes.csv"
    path_public_holidays = "/tmp/s3_files/public_holidays.csv"
    path_school_holidays = "/tmp/s3_files/school_holidays.csv"

    logging.info("Commune transformations")
    df_communes = pd.read_csv(path_communes)
    df_communes.rename(columns={"code": "com"}, inplace=True)

    logging.info("Public holidays transformations")
    df_public_holidays = pd.read_csv(path_public_holidays, sep=",")
    df_public_holidays["date"] = pd.to_datetime(df_public_holidays["date"])

    logging.info("School holidays transformations")
    df_school_holidays = pd.read_csv(path_school_holidays, sep=",")
    df_school_holidays["start_date"] = pd.to_datetime(df_school_holidays["start_date"])
    df_school_holidays["end_date"] = pd.to_datetime(df_school_holidays["end_date"])

    logging.info("Accidents transformations")
    all_accidents_filename = ti.xcom_pull(
        task_ids="transformation_branch.group_accident_data",
        key="concatenated_accidents_csv_path",
    )

    logging.info(f"Accidents transformations filename: {all_accidents_filename}")

    if all_accidents_filename is None:
        logging.error("XCom value for 'concatenated_accidents_csv_path' is None.")

    df = pd.read_csv(all_accidents_filename)
    df_grouped = (
        df.groupby(["jour", "mois", "an", "dep", "com"])
        .size()
        .reset_index(name="nombre_d_accidents")
    )

    # Generate all dates from existing years
    logging.info("Generate all dates from existing years")
    years = df_grouped["an"].unique()
    start_date = f"{min(years)}-01-01"
    end_date = f"{max(years)}-12-31"
    date_range = pd.date_range(start=start_date, end=end_date)

    # Create a DataFrame from the date range
    logging.info("Create a DataFrame from the date range")
    date_df = pd.DataFrame(
        {
            "date": date_range,
            "jour": date_range.day,
            "mois": date_range.month,
            "an": date_range.year,
        }
    )
    date_df["date"] = pd.to_datetime(date_df["date"]).dt.tz_localize("UTC")

    # 📌 Mark public days
    date_df["public_holidays"] = date_df["date"].isin(df_public_holidays["date"])

    # 🏫 Mark vacation days for each zone
    logging.info("Mark vacation days for each zone")
    for zone in ["zone_a", "zone_b", "zone_c"]:
        date_df[zone] = date_df["date"].apply(
            lambda d: any(
                (d >= row["start_date"])
                and (d <= row["end_date"])
                and (zone in row["zone"])
                for _, row in df_school_holidays.iterrows()
            )
        )

    date_df.drop(columns=["date"], inplace=True)
    df_full = pd.DataFrame(
        product(date_df.itertuples(index=False), df_communes["com"]),
        columns=["date_info", "com"],
    )

    logging.warning(df_full.head())

    df_full[
        [
            "jour",
            "mois",
            "an",
            "public_holidays",
            "zone_a",
            "zone_b",
            "zone_c",
        ]
    ] = pd.DataFrame(df_full["date_info"].tolist(), index=df_full.index)
    df_full.drop(columns=["date_info"], inplace=True)
    df_full.to_csv("/tmp/s3_files/df_full.csv")


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
            task_id="accident_data", python_callable=_fetch_files, op_args=["accidents"]
        )
        meta_data = PythonOperator(
            task_id="meta_data", python_callable=_fetch_files, op_args=["meta"]
        )
        [accident_data, meta_data]

    with TaskGroup(group_id="transformation_branch") as transformation_branch:
        group_accident_data = PythonOperator(
            task_id="group_accident_data",
            python_callable=_group_accident_data,
            provide_context=True,
        )
        group_data = PythonOperator(
            task_id="group_data", python_callable=_group_data, provide_context=True
        )
        group_accident_data >> group_data

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    start >> data_branch >> transformation_branch >> end
    # start >> [transformation_branch] >> end
