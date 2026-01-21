# check_dag_windows.py
# Purpose: Create a simple Apache Airflow DAG (TaskFlow style) that runs on Windows.
# The DAG performs three steps:
#   1) Create a file using PowerShell
#   2) Verify that the file exists
#   3) Read and print the file content using Python
#
# This is the Windows/PowerShell equivalent of the common Linux BashOperator example.

from airflow.sdk import dag, task
from datetime import datetime
import os


@dag(
    # Run once per day at midnight
    schedule='@daily',

    # Earliest date Airflow will consider when creating DAG runs
    start_date=datetime(2025, 1, 1),

    # Shown in the Airflow UI
    description='DAG to check data (Windows / PowerShell version)',

    # Team / domain tags for UI filtering
    tags=['data_engineering'],
)
def check_dag_old():
    """
    DAG factory function.

    This function is executed during DAG *parsing* to build the workflow blueprint:
      - define tasks
      - define dependencies

    The actual task code runs later at *runtime* inside workers.
    """

    # --------------------------------------------------
    # Task 1: Create a file using PowerShell
    # --------------------------------------------------
    # PowerShell equivalent of: echo "Hi there!" >/tmp/dummy
    #
    # On Windows:
    # - Use $env:TEMP instead of /tmp
    # - Set-Content creates or overwrites a file
    @task.powershell
    def create_file():
        return 'Set-Content -Path "$env:TEMP\\dummy.txt" -Value "Hi there!"'

    # --------------------------------------------------
    # Task 2: Check that the file exists
    # --------------------------------------------------
    # PowerShell equivalent of: test -f /tmp/dummy
    #
    # Test-Path returns True/False and exits successfully
    # if the file exists
    @task.powershell
    def check_file_exists():
        return 'Test-Path "$env:TEMP\\dummy.txt"'

    # --------------------------------------------------
    # Task 3: Read and print the file using Python
    # --------------------------------------------------
    @task
    def read_file():
        temp_dir = os.environ['TEMP']
        file_path = os.path.join(temp_dir, 'dummy.txt')

        # Read file in binary mode and print to task logs
        with open(file_path, 'rb') as f:
            print(f.read())

    # --------------------------------------------------
    # Dependencies
    # --------------------------------------------------
    # create_file must succeed before check_file_exists
    # check_file_exists must succeed before read_file
    create_file() >> check_file_exists() >> read_file()


# Instantiate / register the DAG
check_dag_old
