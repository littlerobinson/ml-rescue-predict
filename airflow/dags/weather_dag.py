import csv
import logging
import os
import re
import time
from collections import defaultdict
from datetime import datetime, timedelta
from itertools import product

import pandas as pd
import requests
from bs4 import BeautifulSoup
from s3_to_snowflake import S3ToSnowflakeOperator
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


dag_default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}
