from typing import Sequence

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from airflow.hooks.S3_hook import S3Hook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class S3ToSnowflakeOperator(BaseOperator):
    template_fields: Sequence[str] = (
        "bucket",
        "key",
        "table",
        "snowflake_conn_id",
        "aws_conn_id",
    )

    def __init__(
        self,
        bucket: str,
        key: str,
        table: str,
        schema: str,
        snowflake_conn_id: str = "snowflake_default",
        aws_conn_id: str = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket = bucket
        self.key = key
        self.table = table
        self.schema = schema
        self.snowflake_conn_id = snowflake_conn_id
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        # Create snowflake connection
        ctx = snowflake.connector.connect(
            user="alex",
            password="Halt5-Dumpling2-Radar4-Pasted3-Volumes9",
            account="UGPSTAX-HM40418",
            session_parameters={
                "QUERY_TAG": "EndOfMonthFinancials",
            },
        )
        ctx.cursor().execute("CREATE OR REPLACE DATABASE rescue_predict_db")
        ctx.cursor().execute("USE DATABASE rescue_predict_db")
        ctx.cursor().execute("CREATE SCHEMA IF NOT EXISTS public")
        ctx.cursor().execute("USE SCHEMA public")
        """Execute the transfer of data from S3 to Snowflake."""
        task_instance = context["task_instance"]
        execution_date = context["execution_date"]
        dag_id = context["dag"].dag_id
        task_id = context["task"].task_id

        self.log.info(
            f"Starting S3 to Snowflake transfer for table {self.schema}.{self.table} "
            f"(DAG: {dag_id}, Task: {task_id}, Execution Date: {execution_date})"
        )

        # Télécharger le fichier depuis S3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        s3_file_path = s3_hook.download_file(self.key, bucket_name=self.bucket)
        self.log.info(f"Downloaded file {self.key} from S3 bucket {self.bucket}")

        # Lire les données
        df = pd.read_csv(s3_file_path)
        self.log.info(f"Successfully read {len(df)} rows from the S3 file")

        # Ajouter les colonnes de contexte
        row_count = len(df)
        execution_date_str = execution_date.strftime("%Y-%m-%d %H:%M:%S")
        df["execution_date"] = [execution_date_str] * row_count
        df["dag_id"] = [dag_id] * row_count
        df["task_id"] = [task_id] * row_count

        write_pandas(conn=ctx, df=df, auto_create_table=True, table_name=self.table)

        # Envoyer les métriques dans XCom
        task_instance.xcom_push(key="rows_inserted", value=row_count)
        task_instance.xcom_push(key="execution_time", value=str(execution_date))

        self.log.info(
            f"Successfully inserted {row_count} rows into {self.schema}.{self.table} "
            f"at {execution_date}"
        )

        return row_count
