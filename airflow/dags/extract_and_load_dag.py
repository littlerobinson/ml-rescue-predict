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

        s3_hook = S3Hook(aws_conn_id="aws_default")

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
                s3_key = f"{S3_PATH}accidents/{filename}"

                if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
                    file_response = requests.get(file_url)

                    if file_response.status_code == 200:
                        with open(full_path_to_file, "wb") as f:
                            f.write(file_response.content)

                        s3_hook = S3Hook(aws_conn_id="aws_default")
                        s3_hook.load_file(
                            filename=full_path_to_file,
                            key=s3_key,
                            bucket_name=S3_BUCKET_NAME,
                            replace=True,
                        )
                        logging.info(f"Saved accidents data to S3 with name {filename}")
                    else:
                        logging.error(f"Failed to download {filename}")
                else:
                    logging.info(
                        f"File {filename} already exists in S3 ({s3_key}), skipping upload."
                    )

    logging.info("fetch accident data finished")


def _fetch_public_holidays_data(zone="metropole", current_year=None):
    logging.info("fetch public holidays")
    if current_year is None:
        current_year = datetime.now().year

    start_year = current_year - 20
    end_year = current_year + 5

    all_holidays = {}

    filename = "public_holidays.csv"
    full_path_to_file = f"/tmp/{filename}"

    for year in range(start_year, end_year + 1):
        url = f"https://calendrier.api.gouv.fr/jours-feries/{zone}/{year}.json"
        response = requests.get(url)

        if response.status_code == 200:
            holidays = response.json()
            all_holidays.update(holidays)

    if not all_holidays:
        logging.info("holydays dictionary is empty")
    else:
        # Save holidays data as csv
        with open(full_path_to_file, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["date", "public_holiday"])

            for date in sorted(all_holidays.keys()):
                formatted_date = date
                writer.writerow([formatted_date, all_holidays[date]])

        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_key = f"{S3_PATH}meta/{filename}"
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Saved public holidays data to S3 with name {filename}")


def _fetch_fire_brigade():
    def fetch_all(filename, full_path_to_file):
        logging.info("fetch fire brigade")
        departments = list(range(1, 96)) + list(range(971, 977))
        HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }

        fire_brigades = []

        for dept in departments:
            dept_str = str(dept).zfill(2)
            page = 0

            while True:
                API_URL = f"https://pompierama.com/views/ajax?_wrapper_format=drupal_ajax&recherche={dept_str}&view_name=recherche_caserne&view_display_id=block_1&page={page}"
                logging.info("fetch fire brigade")
                try:
                    response = requests.get(API_URL, headers=HEADERS)
                    response.raise_for_status()
                    time.sleep(1)

                    data = response.json()
                    html_data = next(
                        (
                            item["data"]
                            for item in data
                            if "data" in item
                            and isinstance(item["data"], str)
                            and item["data"].strip()
                        ),
                        None,
                    )

                    if not html_data:
                        break

                    soup = BeautifulSoup(html_data, "html.parser")
                    rows = soup.find_all("tr")

                    if len(rows) <= 1:
                        break

                    for row in rows[1:]:
                        cols = row.find_all("td")
                        if len(cols) >= 3:
                            fire_brigades.append(
                                {
                                    "name": cols[0].text.strip(),
                                    "city": cols[1].text.strip(),
                                    "department": cols[2].text.strip(),
                                }
                            )

                    if soup.find("a", rel="next"):
                        page += 1
                    else:
                        break

                except (requests.exceptions.RequestException, Exception):
                    break

        df = pd.DataFrame(fire_brigades)
        df.to_csv(full_path_to_file, index=False, encoding="utf-8")

    filename = "fire_brigade.csv"
    full_path_to_file = f"/tmp/{filename}"
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}meta/{filename}"
    if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        fetch_all(filename, full_path_to_file)
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Saved public holidays data to S3 with name {filename}")


def _fetch_school_holidays_data():
    def get_school_holidays_api(start_year, end_year):
        url = "https://data.education.gouv.fr/api/records/1.0/search/"
        params = {
            "dataset": "fr-en-calendrier-scolaire",
            "q": "",
            "rows": 10000,
            "facet": "annee_scolaire",
            "facet": "metropole",
        }

        response = requests.get(url, params=params)
        data = response.json()

        records = data.get("records", [])
        holidays = []

        for record in records:
            fields = record.get("fields", {})
            school_year = fields.get("annee_scolaire")
            zone = fields.get("zones")
            holiday_type = fields.get("description")
            start_date = fields.get("start_date")
            end_date = fields.get("end_date")

            if start_year <= int(school_year.split("-")[0]) <= end_year:
                holidays.append(
                    {
                        "school_year": school_year,
                        "zone": zone,
                        "type": holiday_type,
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )

        return holidays

    filename = "school_holidays.csv"
    full_path_to_file = f"/tmp/{filename}"
    start_year = datetime.now().year - 3
    end_year = datetime.now().year + 1
    logging.info(f"Fetch school holidays for {start_year} to {end_year}")
    holidays_data = get_school_holidays_api(start_year, end_year)
    df = pd.DataFrame(holidays_data)
    df = df.drop("school_year", axis=1)
    df.to_csv(full_path_to_file, index=False)

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}meta/{filename}"

    s3_hook.load_file(
        filename=full_path_to_file,
        key=s3_key,
        bucket_name=S3_BUCKET_NAME,
        replace=True,
    )
    logging.info(f"Saved school holidays data to S3 with name {filename}")


# Function to fetch data from the API
def _fetch_communes():
    base_url = "https://geo.api.gouv.fr/departements"
    save_dir = "/tmp"
    os.makedirs(save_dir, exist_ok=True)

    all_communes = []

    # Loop through all departments (1 to 95 for metropolitan France)
    for dept in range(1, 96):
        url = f"{base_url}/{dept}/communes"
        params = {"fields": "nom,codeRegion,code,population", "format": "json"}

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            communes = response.json()
            all_communes.extend(communes)
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data for department {dept}: {e}")

    # Extract commune names and postal codes
    data = []
    for commune in all_communes:
        nom_commune = commune["nom"]
        code_region = commune["codeRegion"]
        code = commune["code"]
        population = commune["population"]
        data.append([nom_commune, code_region, code, population])

    # Create DataFrame
    df_communes = pd.DataFrame(
        data, columns=["commune", "code_region", "code", "population"]
    )

    # Save as CSV
    filename = "all_communes.csv"
    full_path_to_file = f"/tmp/{filename}"
    save_path = os.path.join(save_dir, "all_communes.csv")
    df_communes.to_csv(save_path, index=False, encoding="utf-8")

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}meta/{filename}"
    if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )
        logging.info(f"Communes file saved successfully: {save_path}")


def _fetch_weather_history():
    filename = "weather_history.csv"
    full_path_to_file = "/opt/airflow/data/meteo_celsius_Quiers_2022_2023.csv"

    # Upload to S3
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PATH}meta/{filename}"
    if not s3_hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET_NAME):
        s3_hook.load_file(
            filename=full_path_to_file,
            key=s3_key,
            bucket_name=S3_BUCKET_NAME,
            replace=True,
        )


with DAG(
    "extract_and_load",
    default_args=dag_default_args,
    description="Download a CSV file and load it into an S3 bucket",
    schedule_interval="0 0 30 12 *",  # At 00:00 on day-of-month 30 in December.
    tags=["ELT", "extract", "load", "bucket"],
) as dag:
    logging.info("extract_and_load")

    start = BashOperator(task_id="start", bash_command="echo 'Start!'")

    with TaskGroup(group_id="accident_branch") as accident_branch:
        fetch_accident_data = PythonOperator(
            task_id="fetch_accident_data",
            python_callable=_fetch_accident_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        [fetch_accident_data]

    with TaskGroup(group_id="meta_branch") as meta_branch:
        fetch_public_holidays_data = PythonOperator(
            task_id="fetch_public_holidays_data",
            python_callable=_fetch_public_holidays_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_school_holidays_data = PythonOperator(
            task_id="fetch_school_holidays_data",
            python_callable=_fetch_school_holidays_data,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_fire_brigade = PythonOperator(
            task_id="fetch_fire_brigade",
            python_callable=_fetch_fire_brigade,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_weather_history = PythonOperator(
            task_id="fetch_weather_history",
            python_callable=_fetch_weather_history,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        fetch_communes = PythonOperator(
            task_id="fetch_communes",
            python_callable=_fetch_communes,
            retries=1,
            retry_delay=timedelta(minutes=10),
        )
        [
            fetch_public_holidays_data,
            fetch_school_holidays_data,
            fetch_fire_brigade,
            fetch_weather_history,
            fetch_communes,
        ]

    end = BashOperator(task_id="end", bash_command="echo 'End!'")

    start >> [accident_branch, meta_branch] >> end
