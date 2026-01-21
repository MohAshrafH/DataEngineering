"""sensor_xcom_dag.py

OVERVIEW (what this file is)
----------------------------
This file defines an Apache Airflow DAG (workflow) that demonstrates:
- A *sensor task* written using the TaskFlow API (@task.sensor)
- Passing data from the sensor to a downstream task using XCom

What it does (flow)
-------------------
1) The sensor task calls a public HTTP API (Dog CEO).
2) If the API returns HTTP 200, the sensor:
   - marks the condition as met (is_done=True)
   - returns the API JSON as xcom_value (stored in XCom)
3) If the API does NOT return HTTP 200, the sensor:
   - keeps waiting (is_done=False) and tries again later
4) The downstream task receives the sensor output (from XCom) and prints it.

Execution picture
-----------------
check_dog_availability (sensor)  -->  print_dog_picture_url (task)

"""

# Import DAG and TaskFlow decorators.
# - @dag: turns a Python function into an Airflow DAG definition.
# - @task: turns a Python function into an Airflow task.
from airflow.decorators import dag, task

# Import datetime to specify the DAG start_date (Airflow scheduling uses logical dates).
from datetime import datetime

# requests is a Python library for making HTTP calls.
import requests

# PokeReturnValue is an Airflow object used by sensor tasks.
# It tells Airflow:
# - is_done: whether the sensor condition is satisfied (True/False)
# - xcom_value: data to store in XCom when the sensor succeeds
from airflow.sensors.base import PokeReturnValue


# Define the DAG using the @dag decorator.
# start_date: earliest logical date Airflow uses for this DAG.
# schedule='@daily': create one logical run per day.
# catchup=False: do not create backfill runs for past dates.
@dag(start_date=datetime(2022, 12, 1), schedule="@daily", catchup=False)
def sensor_xcom_dag():
    """DAG factory function.

    Airflow imports this file and runs this function to build the DAG structure
    (tasks + dependencies). The code inside defines what tasks exist and how
    they connect.
    """

    # Define a sensor task using @task.sensor.
    # A sensor task is a task that repeatedly checks a condition until it becomes True.
    #
    # poke_interval=30:
    #   Wait 30 seconds between condition checks.
    # timeout=3600:
    #   Give up after 3600 seconds (1 hour) if the condition never becomes True.
    # mode='poke':
    #   Keep the task running while waiting (it occupies a worker slot).
    @task.sensor(poke_interval=30, timeout=3600, mode="poke")
    def check_dog_availability() -> PokeReturnValue:
        """Sensor condition function.

        Goal:
        - Call the Dog CEO API and wait until it returns HTTP 200.

        Return:
        - PokeReturnValue(is_done=..., xcom_value=...)
          is_done=True  -> sensor succeeds and downstream tasks can run
          is_done=False -> sensor keeps waiting and will poke again
          xcom_value    -> data saved into XCom when is_done=True
        """

        # Make an HTTP GET request to the Dog CEO API.
        # This API returns JSON containing a random dog image URL.
        r = requests.get("https://dog.ceo/api/breeds/image/random")

        # Print the HTTP status code (useful for debugging in task logs).
        # 200 means success.
        print(r.status_code)

        # If the request succeeded, mark the sensor condition as met.
        if r.status_code == 200:
            # condition_met=True tells Airflow the sensor is done.
            condition_met = True

            # r.json() parses the HTTP response body into a Python dict.
            # Typical response shape:
            #   {"status": "success", "message": "https://images.dog.ceo/...jpg"}
            operator_return_value = r.json()
        else:
            # Any non-200 status means the condition is not met yet.
            condition_met = False

            # No data to pass because we did not succeed.
            operator_return_value = None

            # Log why the sensor is not done yet.
            print(f"Dog URL returned the status code {r.status_code}")

        # Return PokeReturnValue.
        # - is_done controls whether the sensor finishes or keeps waiting.
        # - xcom_value is saved to XCom only when is_done=True.
        return PokeReturnValue(is_done=condition_met, xcom_value=operator_return_value)

    # Define a normal Python task using @task.
    # This task will receive the sensor output from XCom as an argument.
    @task
    def print_dog_picture_url(url):
        """Downstream task.

        Input:
        - url: the value returned by the sensor (stored in XCom).

        Behavior:
        - prints the received JSON to task logs.
        """

        # Print whatever was received.
        # In this DAG, it will likely print a dict like:
        #   {'status': 'success', 'message': 'https://images.dog.ceo/...jpg'}
        print(url)

    # Wire tasks together and pass data using TaskFlow syntax.
    #
    # check_dog_availability() creates the sensor task node.
    # Its output (xcom_value) becomes the input to print_dog_picture_url(...).
    # This establishes both:
    # - dependency: sensor runs before print task
    # - data passing: sensor output -> print task input (via XCom)
    print_dog_picture_url(check_dog_availability())


# Instantiate/register the DAG so Airflow can discover it.
# Airflow discovers DAGs by importing Python files; this call creates the DAG object.
sensor_xcom_dag()
