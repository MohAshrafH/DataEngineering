"""
OVERVIEW
--------
This DAG demonstrates how to READ an Airflow Variable at runtime
from within a task using the Airflow SDK Variable API.

CONTEXT
-------
- The variable being fetched is named: `api`
- It was previously created via:
  - Airflow UI (Admin → Variables), OR
  - Environment variable (AIRFLOW_VAR_API)
- The value is stored as JSON and automatically encrypted at rest
  (in Astro-managed environments)

IMPORTANT NOTES
---------------
- This DAG does NOT validate whether the variable contains real credentials
- It only tests variable retrieval and JSON deserialization
- The printed output will appear in task logs, not in the UI
"""

# Import DAG and task decorators from the Airflow SDK
# These decorators define DAG structure and task behavior
from airflow.sdk import dag, task

# Import Variable API to read Airflow Variables
# Variables are key-value pairs stored in Airflow's metadata database
from airflow.sdk import Variable


# The @dag decorator tells Airflow to treat this function as a DAG definition
@dag(
    # No schedule: DAG is intended for manual execution and testing
    schedule=None,

    # A start_date is required for DAG registration
    # No backfill will occur because schedule=None
    start_date=None,

    # Tags are metadata only and help with UI filtering
    tags=["variables", "sdk", "config"],
)
def variable():
    """
    DAG FUNCTION
    ------------
    This function defines the tasks and their execution order.
    It is evaluated by Airflow at DAG parse time.
    """

    # Define a Python task using the @task decorator
    # This creates a task that will execute inside an Airflow worker
    @task
    def print_my_var():
        """
        TASK FUNCTION
        -------------
        This task retrieves an Airflow Variable named `api` and prints it.
        """

        # Variable.get(...) fetches the variable value by key
        # - "api" is the variable key
        # - deserialize_json=True tells Airflow to:
        #   * Parse the stored string as JSON
        #   * Return a Python dict instead of a raw string
        #
        # Example stored value:
        # {
        #   "url": "http://myapi.com/",
        #   "endpoint": "v1"
        # }
        api_config = Variable.get("api", deserialize_json=True)

        # Print the variable contents
        # Output will be visible in the task logs
        print(api_config)

    # Invoke the task
    # This creates a task instance and registers it in the DAG
    print_my_var()


# Instantiate the DAG
# Required when using the @dag decorator pattern
variable()
