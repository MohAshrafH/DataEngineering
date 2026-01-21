"""s3_sensor_wait_for_file.py

OVERVIEW (what this code is)
- This file defines a single Apache Airflow DAG (workflow).
- The DAG demonstrates the most common "sensor" pattern with AWS S3:
  1) WAIT: Use S3KeySensor to keep checking an S3 bucket until a target object exists.
  2) CONTINUE: Once the object is found, run a downstream task (here, a simple print).

WHY THIS DAG EXISTS (the problem it solves)
- In real pipelines, upstream systems often upload data to S3 at an unknown time.
- If your pipeline starts before the file exists, downstream processing fails.
- A sensor is a safe gatekeeper: it prevents the DAG from continuing until data is ready.

WHAT THIS DAG WILL WAIT FOR (your specific setup)
- Bucket: airflow-s3-test-moh-2026
- Object (file): data_1.txt
- Location inside the bucket: the *root* of the bucket (no folder prefix), so the key is "data_1.txt".

FLOW (high level)
S3KeySensor (wait_for_s3_object)  -->  Python task (process_file)
"""

# Import the DAG and task decorators.
# - @dag turns a Python function into an Airflow DAG definition.
# - @task turns a Python function into an Airflow task.
from airflow.decorators import dag, task

# Import the S3 sensor from the Amazon provider package.
# - airflow.providers.amazon... is only available if you installed the Amazon provider
#   (e.g., apache-airflow-providers-amazon in requirements.txt).
# - S3KeySensor is a "sensor": it waits until a condition is True.
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

# datetime is used to define start_date (the earliest logical scheduling date for the DAG).
from datetime import datetime


# @dag(...) decorates the function below so Airflow treats it as a DAG definition.
@dag(
    # schedule=None means: "do not run on a schedule automatically".
    # You will trigger it manually from the Airflow UI.
    schedule=None,

    # start_date is required by Airflow for scheduling metadata.
    # Even though schedule=None, Airflow still needs a start_date to define the DAG's timeline.
    start_date=datetime(2023, 1, 1),

    # tags are labels shown in the Airflow UI. They help you filter and organize DAGs.
    tags=["aws", "s3", "sensor"],

    # Optional: a human-readable description (shows in UI).
    description="Wait for an S3 object using S3KeySensor, then run a downstream task.",
)

def s3_sensor_wait_for_file():
    """DAG definition function.

    Everything created inside this function becomes part of the DAG:
    - operators/sensors/tasks
    - dependencies between tasks
    """

    # -------------------------------------------------------------------------
    # 1) SENSOR TASK: wait until the S3 object exists
    # -------------------------------------------------------------------------

    # Create a sensor task object.
    # S3KeySensor keeps calling AWS S3 APIs until the target key is found.
    wait_for_s3_object = S3KeySensor(
        # task_id is the unique name of this task inside the DAG.
        task_id="wait_for_s3_object",

        # aws_conn_id is the *Airflow Connection ID* (defined in Airflow UI: Admin -> Connections).
        # - This is NOT something you find in AWS.
        # - It is a name you created in Airflow to store AWS credentials (access key / secret key)
        #   and region.
        # Example: you might have created a connection with Connection ID = "aws_s3_test".
        aws_conn_id="aws_s3_test",

        # bucket_name is the S3 bucket that contains the object you are waiting for.
        # This is your bucket name in AWS.
        bucket_name="airflow-s3-test-moh-2026",

        # bucket_key is the S3 object's "key" (its full name inside the bucket).
        # - In S3, an object name can look like folders, e.g., "incoming/data_1.txt".
        # - Because you uploaded data_1.txt at the root of the bucket (no folder),
        #   the key is simply "data_1.txt".
        bucket_key="data_1.txt",

        # wildcard_match controls whether bucket_key is treated like a wildcard pattern.
        # - False: exact match only ("data_1.txt" must exist)
        # - True: pattern match (e.g., "data_*.txt" would match many files)
        # Here we set False because we want to wait for a single, specific object.
        wildcard_match=False,

        # poke_interval controls how often the sensor checks S3.
        # Example: 15 means "check every 15 seconds".
        poke_interval=15,

        # timeout controls the maximum time (in seconds) the sensor will keep waiting.
        # If the file never appears, the task fails after this time.
        # Example: 10 minutes = 600 seconds.
        timeout=600,

        # mode controls how the sensor occupies Airflow worker resources while waiting.
        # - "poke": the task stays running and sleeps between checks (uses a worker slot).
        # - "reschedule": the task frees the worker slot between checks (better for many sensors).
        # For local testing, either is fine; reschedule is often better practice.
        mode="reschedule",
    )

    # -------------------------------------------------------------------------
    # 2) DOWNSTREAM TASK: run after the object exists
    # -------------------------------------------------------------------------

    # @task turns this function into an Airflow task.
    # It will only run after the sensor succeeds.
    @task
    def process_file():
        """A placeholder processing step.

        In real pipelines, you might:
        - download the object
        - parse/transform it
        - load it into a database/warehouse
        """

        # This print goes to Airflow task logs.
        # It proves the pipeline progressed past the sensor.
        print("S3KeySensor found 'data_1.txt' in bucket 'airflow-s3-test-moh-2026'.")
        print("Now continuing with downstream processing...")

    # Calling the function creates the actual task node in the DAG.
    process_file_task = process_file()

    # -------------------------------------------------------------------------
    # 3) DEPENDENCY (task order)
    # -------------------------------------------------------------------------

    # The >> operator defines task order in Airflow.
    # wait_for_s3_object must succeed BEFORE process_file_task runs.
    wait_for_s3_object >> process_file_task


# This line instantiates (creates) the DAG object so Airflow can discover it.
# Airflow scans the 'dags/' folder, imports this file, and looks for DAG objects.
s3_sensor_wait_for_file()
