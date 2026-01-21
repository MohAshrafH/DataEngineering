from airflow import DAG
from airflow.sensors.python import PythonSensor
from datetime import datetime

# -----------------------------------------------------------------------------
# OVERVIEW (WHAT THIS FILE IS AND WHY IT EXISTS)
# -----------------------------------------------------------------------------
# This file defines a very small Apache Airflow workflow (called a DAG).
#
# Purpose of this DAG:
# - Demonstrate what a PythonSensor is
# - Demonstrate how a sensor "waits" instead of doing work
# - Show how Airflow repeatedly checks a condition until it becomes True
#
# High-level behavior:
# - Airflow schedules this DAG to run once per day
# - The DAG contains only ONE task: a PythonSensor
# - The PythonSensor keeps calling a Python function
# - The function always returns False
# - Because the condition never becomes True, the sensor keeps waiting
# - After a fixed amount of time, the sensor fails due to timeout
# -----------------------------------------------------------------------------


# -----------------------------------------------------------------------------
# CONDITION FUNCTION (WHAT THE SENSOR IS WAITING FOR)
# -----------------------------------------------------------------------------
# This function represents the condition that must be satisfied
# before the workflow can continue.
#
# IMPORTANT CONCEPT:
# - The sensor does NOT receive the result of this function upfront
# - Instead, Airflow will call this function again and again over time
#
# Expected return values:
# - True  → condition is met → sensor succeeds
# - False → condition is NOT met → sensor keeps waiting
#
# In real projects, this function usually checks something external,
# such as a file, database record, API response, or job status.
def _condition():
    # This implementation always returns False.
    # Meaning:
    # - The condition is never satisfied
    # - The sensor will never succeed naturally
    # - The task will eventually fail due to timeout
    return False


# -----------------------------------------------------------------------------
# DAG DEFINITION (WORKFLOW METADATA)
# -----------------------------------------------------------------------------
# The DAG object defines the workflow itself:
# - its name
# - when it should run
# - how scheduling behaves
#
# The "with" statement is a context manager:
# - Any tasks defined inside this block automatically belong to this DAG
# - No execution happens here; this is only workflow DEFINITION time
with DAG(
    dag_id="sensor",                    # Unique identifier for this DAG in Airflow

    # Logical start date used by the scheduler to calculate run dates.
    # This does NOT mean the DAG actually ran on this date.
    start_date=datetime(2021, 1, 1),

    # Tells Airflow to create one DAG run per day.
    schedule="@daily",

    # Prevent Airflow from creating historical runs between start_date
    # and the current date.
    catchup=False,
):

    # -------------------------------------------------------------------------
    # PYTHONSENSOR TASK (THE WAITING LOGIC)
    # -------------------------------------------------------------------------
    # This task does not process data.
    # Its only responsibility is to WAIT until a condition becomes True.
    waiting_for_condition = PythonSensor(

        # Unique name for this task inside the DAG
        task_id="waiting_for_condition",

        # Reference to the Python function that checks the condition.
        # Airflow will call this function repeatedly as:
        #     _condition()
        python_callable=_condition,

        # Time (in seconds) to wait between consecutive checks.
        # Here: check once every 60 seconds.
        poke_interval=60,

        # Maximum total time the sensor is allowed to wait.
        # If this time is exceeded and the condition is still False,
        # the task fails with a timeout error.
        timeout=7 * 24 * 60 * 60,  # 7 days expressed in seconds
    )

    # -------------------------------------------------------------------------
    # DAG STRUCTURE
    # -------------------------------------------------------------------------
    # No downstream or upstream tasks are defined.
    # This DAG consists of a single sensor task only.
    # The entire workflow is effectively:
    #     start → waiting_for_condition → end
