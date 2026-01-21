"""s3_sensor_wildcard.py

OVERVIEW
- This DAG demonstrates using S3KeySensor with a wildcard pattern.
- The sensor waits until ANY object whose name starts with 'data_' exists in the S3 bucket.
- Once a matching object appears, the downstream task runs.

CONTEXT (your setup)
- AWS connection ID in Airflow: aws_s3
- S3 bucket: airflow-s3-test-moh-2026
- Object naming pattern: data_* (for example: data_1.txt, data_2026.csv)

FLOW
S3KeySensor (wait_for_data_files)  -->  process_file
"""

# Import DAG and task decorators used to define workflows and tasks in Airflow
from airflow.sdk import dag, task

# Import the S3KeySensor from the Amazon provider package
# This sensor polls Amazon S3 until a matching object is found
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# Import datetime to define the DAG's logical start date
from datetime import datetime


@dag(
    # No schedule: the DAG is triggered manually
    schedule=None,

    # Required logical start date for Airflow's scheduler
    start_date=datetime(2023, 1, 1),

    # Tags help group and filter DAGs in the Airflow UI
    tags=["aws", "s3", "sensor"],

    # Do not backfill historical runs
    catchup=False,
)

def s3_sensor_wildcard_dag():
    """DAG definition function."""

    # ------------------------------------------------------------------
    # SENSOR: wait for any S3 object whose key matches 'data_*'
    # ------------------------------------------------------------------

    wait_for_data_files = S3KeySensor(
        # Unique task name in the DAG
        task_id="wait_for_data_files",

        # Airflow connection ID that stores AWS credentials
        aws_conn_id="aws_s3_test",

        # Name of your S3 bucket in AWS
        bucket_name="airflow-s3-test-moh-2026",

        # Wildcard pattern for object keys inside the bucket
        # This matches any object whose name starts with 'data_'
        bucket_key="data_*",

        # Enable wildcard interpretation of bucket_key
        wildcard_match=True,

        # Check every 15 seconds
        poke_interval=15,

        # Give up after 10 minutes if no matching object appears
        timeout=600,

        # Free the worker slot between checks (recommended for sensors)
        mode="reschedule",
    )

    # ------------------------------------------------------------------
    # DOWNSTREAM TASK: runs after a matching object is detected
    # ------------------------------------------------------------------

    @task
    def process_file():
        """Placeholder processing step."""
        print("A file matching 'data_*' was found in S3.")
        print("Continuing pipeline execution...")

    # Instantiate the downstream task
    process_file_task = process_file()

    # Define task dependency
    wait_for_data_files >> process_file_task


# Instantiate the DAG so Airflow can discover it
s3_sensor_wildcard_dag()
