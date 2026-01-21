"""
OVERVIEW
--------
This DAG demonstrates how Apache Airflow *would* execute a SQL query against Snowflake
using the generic SQLExecuteQueryOperator.

IMPORTANT DISCLAIMER
--------------------
This DAG is NOT expected to run successfully in your current environment because:
- There is NO real Snowflake account, database, schema, or table configured
- There is NO valid Snowflake Airflow connection backed by real credentials

The purpose of this DAG is *educational and structural only*:
- To validate that the Snowflake provider is installed correctly
- To test that Airflow can *resolve* a Snowflake connection defined via
  environment variables or the UI
- To confirm DAG parsing and operator wiring, not data execution

In other words: this tests *connection creation and operator setup*,
NOT Snowflake data access.
"""

# Import the DAG decorator from the Airflow SDK
# This is the modern, decorator-based way to define DAGs in Airflow 2.4+
from airflow.sdk import dag

# Import a generic SQL operator that works with many databases
# The actual database used depends entirely on the Airflow connection type
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# The @dag decorator converts this Python function into an Airflow DAG object
# Airflow will scan this file, detect the decorator, and register the DAG
@dag(
    # No schedule is defined because this DAG is intended for manual testing
    schedule=None,

    # A static start_date is required so Airflow can register the DAG
    # It does NOT mean historical backfills will run (since schedule=None)
    start_date=None,

    # Tags are metadata only; they help with filtering in the Airflow UI
    tags=["snowflake", "provider-test", "connections"],
)
def snowflake():
    """
    DAG FUNCTION
    ------------
    Everything inside this function defines tasks and dependencies.
    The function itself is NOT executed by Python directly;
    Airflow calls it during DAG parsing to build the DAG structure.
    """

    # Define a single task that executes a SQL statement
    # The operator itself is database-agnostic; the actual backend
    # is determined by the connection referenced via conn_id
    run_a_query = SQLExecuteQueryOperator(

        # task_id must be unique within the DAG
        # This is how the task is identified in the Airflow UI and logs
        task_id="run_a_query",

        # The SQL statement to be executed
        # In a real Snowflake setup, this table must exist
        # Here, it is intentionally a placeholder
        sql="SELECT * FROM my_table",

        # Reference to an Airflow Connection
        # Airflow will look for a connection with:
        #   conn_id = "snowflake"
        #   conn_type = "snowflake"
        #
        # In this test scenario, the connection may be:
        # - Defined via environment variables (AIRFLOW_CONN_SNOWFLAKE)
        # - Created manually in the Airflow UI
        #
        # Even if credentials are fake, this allows you to test:
        # - Provider installation
        # - Connection resolution
        # - DAG parsing and task instantiation
        conn_id="snowflake",
    )

    # No downstream dependencies are defined because
    # this DAG focuses on a single operator test


# Instantiate the DAG object
# This line is mandatory when using the @dag decorator
snowflake()
