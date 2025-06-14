import logging
import boto3
import time
from datetime import datetime
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.ec2 import (
    EC2CreateInstanceOperator,
    EC2TerminateInstanceOperator,
)
from airflow.hooks.base import BaseHook
import paramiko
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Airflow connexions
aws_conn = BaseHook.get_connection("aws_default")
snowflake_conn = BaseHook.get_connection("snowflake_rescue_predict_db")

# Jenkins Configuration: Load from Airflow Variables
JENKINS_URL = Variable.get("JENKINS_URL")
JENKINS_USER = Variable.get("JENKINS_USER")
JENKINS_TOKEN = Variable.get("JENKINS_TOKEN")
JENKINS_JOB_NAME = Variable.get("JENKINS_JOB_NAME")

# Get AWS connection details from Airflow
KEY_PAIR_NAME = Variable.get("KEY_PAIR_NAME")
KEY_PATH = Variable.get("KEY_PATH")  # Path to your private key inside the container
AMI_ID = Variable.get("AMI_ID")
SECURITY_GROUP_ID = Variable.get("SECURITY_GROUP_ID")
INSTANCE_TYPE = Variable.get("INSTANCE_TYPE")
aws_conn = BaseHook.get_connection("aws_default")  # Use the Airflow AWS connection
aws_access_key_id = aws_conn.login
aws_secret_access_key = aws_conn.password
region_name = aws_conn.extra_dejson.get(
    "region_name", "eu-west-3"
)  # Default to 'eu-west-3'

# Retrieve other env variables for MLFlow to run
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")
MLFLOW_EXPERIMENT_ID = Variable.get("MLFLOW_EXPERIMENT_ID")
MLFLOW_LOGGED_MODEL = Variable.get("MLFLOW_LOGGED_MODEL")
AWS_ACCESS_KEY_ID = aws_access_key_id
AWS_SECRET_ACCESS_KEY = aws_secret_access_key

SNOWFLAKE_USER = Variable.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = Variable.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = Variable.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = Variable.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA")
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_ROLE = ""

# DB_USER = snowflake_conn.login
# DB_PASSWORD = snowflake_conn.password
# DB_HOST = snowflake_conn.host
# DB_PORT = snowflake_conn.port
# DB_NAME = snowflake_conn.schema


if not all([JENKINS_URL, JENKINS_USER, JENKINS_TOKEN]):
    raise ValueError("Missing one or more Jenkins configuration environment variables")

# DAG Configuration
DAG_ID = "jenkins_ec2_ml_training_dag"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "retries": 1,
}

# Define the DAG
with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    default_args=default_args,
    description="Poll Jenkins, launch EC2, and run ML training",
    catchup=False,
    tags=["jenkins", "ec2", "ml-training"],
) as dag:
    # Step 1: Poll Jenkins Job Status
    # @task
    # def poll_jenkins_job():
    #     """Poll Jenkins for the job status and check for successful build."""
    #     import requests
    #     import time

    #     # Step 1: Get the latest build number from the job API
    #     job_url = f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/api/json"
    #     response = requests.get(job_url, auth=(JENKINS_USER, JENKINS_TOKEN))
    #     if response.status_code != 200:
    #         raise Exception(f"Failed to query Jenkins API: {response.status_code}")

    #     job_info = response.json()
    #     latest_build_number = job_info["lastBuild"]["number"]

    #     # Step 2: Poll the latest build's status
    #     build_url = (
    #         f"{JENKINS_URL}/job/{JENKINS_JOB_NAME}/{latest_build_number}/api/json"
    #     )

    #     while True:
    #         response = requests.get(build_url, auth=(JENKINS_USER, JENKINS_TOKEN))
    #         if response.status_code == 200:
    #             build_info = response.json()
    #             if not build_info["building"]:  # Build is finished
    #                 if build_info["result"] == "SUCCESS":
    #                     print("Jenkins build successful!")
    #                     return True
    #                 else:
    #                     raise Exception("Jenkins build failed!")
    #         else:
    #             raise Exception(f"Failed to query Jenkins API: {response.status_code}")

    #         time.sleep(30)  # Poll every 30 seconds
    with TaskGroup(group_id="training_branch") as training_branch:
        create_ec2_instance = EC2CreateInstanceOperator(
            task_id="create_ec2_instance",
            region_name=region_name,
            image_id=AMI_ID,
            max_count=1,
            min_count=1,
            config={  # Dictionary for arbitrary parameters to the boto3 `run_instances` call
                "InstanceType": INSTANCE_TYPE,
                "KeyName": KEY_PAIR_NAME,
                "SecurityGroupIds": [SECURITY_GROUP_ID],
                "TagSpecifications": [
                    {
                        "ResourceType": "instance",
                        "Tags": [{"Key": "Purpose", "Value": "ML-Training"}],
                    }
                ],
            },
            wait_for_completion=True,  # Wait for the instance to be running before proceeding
        )

        @task
        def check_ec2_status(instance_id):
            """Check if the EC2 instance has passed both status checks (2/2 checks passed)."""

            ec2_client = boto3.client(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )
            passed_checks = False

            while not passed_checks:
                # Get the instance status
                response = ec2_client.describe_instance_status(InstanceIds=instance_id)

                # Check if there is any status information returned
                if response["InstanceStatuses"]:
                    instance_status = response["InstanceStatuses"][0]

                    system_status = instance_status["SystemStatus"]["Status"]
                    instance_status_check = instance_status["InstanceStatus"]["Status"]

                    # Log the current status
                    logging.info(
                        f"System Status: {system_status}, Instance Status: {instance_status_check}"
                    )

                    # Check if both status checks are passed
                    if system_status == "ok" and instance_status_check == "ok":
                        logging.info(
                            f"Instance {instance_id} has passed 2/2 status checks."
                        )
                        passed_checks = True
                    else:
                        logging.info(
                            f"Waiting for instance {instance_id} to pass 2/2 status checks..."
                        )
                else:
                    logging.info(
                        f"No status available for instance {instance_id} yet. Waiting..."
                    )

                # Wait before polling again
                time.sleep(15)

            return True

        @task
        def get_ec2_public_ip(instance_id):
            """Retrieve the EC2 instance public IP for SSH."""

            # Initialize the EC2 resource using boto3 with credentials from Airflow connection
            ec2 = boto3.resource(
                "ec2",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
            )

            # Access EC2 instance by ID
            instance = ec2.Instance(instance_id[0])

            # Wait for the instance to be running
            instance.wait_until_running()
            instance.reload()

            # Get the instance's public IP
            public_ip = instance.public_ip_address
            logging.info(f"Public IP of EC2 Instance: {public_ip}")

            # Return the public IP for the SSH task
            return public_ip

        ec2_public_ip = get_ec2_public_ip(create_ec2_instance.output)
        check_ec2_instance = check_ec2_status(create_ec2_instance.output)

        logging.info(f"ec2 instance output {create_ec2_instance.output}")

        @task
        def run_training_via_paramiko(public_ip):
            """Use Paramiko to SSH into the EC2 instance and run ML training."""

            print("PUBLIC IP:", public_ip)
            # Initialize SSH client
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(
                paramiko.AutoAddPolicy()
            )  # Automatically add unknown hosts

            # Load private key
            private_key = paramiko.RSAKey.from_private_key_file(KEY_PATH)

            try:
                # Establish an SSH connection
                ssh_client.connect(
                    hostname=public_ip, username="ubuntu", pkey=private_key
                )
                # TODO : change with connection variables
                # export SNOWFLAKE_USER={snowflake_conn.login}
                # export SNOWFLAKE_PASSWORD={snowflake_conn.password}
                # export SNOWFLAKE_WAREHOUSE={snowflake_conn.warehouse}
                # export SNOWFLAKE_SCHEMA={snowflake_conn.schema}
                # export SNOWFLAKE_DATABASE={snowflake_conn.database}
                # export SNOWFLAKE_ROLE={snowflake_conn.role}
                command = f"""
                export MLFLOW_TRACKING_URI="{MLFLOW_TRACKING_URI}"
                export MLFLOW_EXPERIMENT_ID="{MLFLOW_EXPERIMENT_ID}"
                export MLFLOW_LOGGED_MODEL="{MLFLOW_LOGGED_MODEL}"
                export MLFLOW_EXPERIMENT_NAME="jedha-lead"
                export AWS_ACCESS_KEY_ID="{AWS_ACCESS_KEY_ID}"
                export AWS_SECRET_ACCESS_KEY="{AWS_SECRET_ACCESS_KEY}"
                export SNOWFLAKE_ACCOUNT="{SNOWFLAKE_ACCOUNT}"
                export SNOWFLAKE_USER="{SNOWFLAKE_USER}"
                export SNOWFLAKE_PASSWORD="{SNOWFLAKE_PASSWORD}"
                export SNOWFLAKE_WAREHOUSE="{SNOWFLAKE_WAREHOUSE}"
                export SNOWFLAKE_SCHEMA="{SNOWFLAKE_SCHEMA}"
                export SNOWFLAKE_DATABASE="{SNOWFLAKE_DATABASE}"
                export SNOWFLAKE_ROLE="{SNOWFLAKE_ROLE}"
                git clone https://github.com/littlerobinson/ml-rescue-predict.git
                cd ml-rescue-predict/training
                docker build -t ml-rescue-predict-training .
                cp run.sh.example run.sh
                chmod +x run.sh
                ./run.sh
                """

                # Run your training command via SSH
                stdin, stdout, stderr = ssh_client.exec_command(command)

                # stdout_text = ""
                # stderr_text = ""

                # while not stdout.channel.exit_status_ready():
                #     if stdout.channel.recv_ready():
                #         stdout_text += stdout.channel.recv(1024).decode()
                #     if stdout.channel.recv_stderr_ready():
                #         stderr_text += stdout.channel.recv_stderr(1024).decode(
                #             "utf-8", errors="ignore"
                #         )

                #     time.sleep(5)  # Wait before verify a new time

                # # Read outputs
                # stdout_text += stdout.read().decode()
                # stderr_text += stderr.read().decode()

                # # Display outputs
                # logging.info(stdout_text)
                # logging.error(stderr_text)

                # Wait for the command to complete
                exit_status = stdout.channel.recv_exit_status()

                # Read the outputs
                stdout_text = stdout.read().decode()
                stderr_text = stderr.read().decode()

                # Log the outputs
                logging.info(f"STDOUT:\n{stdout_text}")
                logging.error(f"STDERR:\n{stderr_text}")

                if exit_status != 0:
                    logging.error(f"Command failed with exit status: {exit_status}")
                else:
                    logging.info("Command executed successfully.")

                # logging.info("Command executed successfully.")

            except Exception as e:
                logging.error(f"Error occurred during SSH: {str(e)}")
                raise
            finally:
                # Close the SSH connection
                logging.info("Close the SSH connection")
                ssh_client.close()

        run_training = run_training_via_paramiko(ec2_public_ip)

        terminate_instance = EC2TerminateInstanceOperator(
            task_id="terminate_ec2_instance",
            region_name=region_name,
            instance_ids=create_ec2_instance.output,
            wait_for_completion=True,
            trigger_rule=TriggerRule.ALL_SUCCESS,
        )

        (
            create_ec2_instance
            >> ec2_public_ip
            >> check_ec2_instance
            >> run_training
            >> terminate_instance
        )
