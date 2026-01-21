# my_dag.py
# Purpose: Define an Apache Airflow DAG using TaskFlow (the modern “Python-first” style):
#   - `@dag(...)` defines the workflow metadata (schedule, start date, tags, etc.)
#   - `@task` turns a Python function into an Airflow task
#
# This file contains NO classic Operators (e.g., PythonOperator). It focuses only on the “new way”.


# Import the DAG decorator and the TaskFlow task decorator.
#
# What is a decorator (in Python)?
# - A decorator is syntax that wraps a function to add behavior.
# - `@dag(...)` wraps a function and makes it produce/register an Airflow DAG.
# - `@task` wraps a function and makes it produce/register an Airflow task.
from airflow.sdk import dag, task

# Pendulum datetime is recommended by Airflow.
# - Airflow uses start dates and schedules to create “DAG runs” over time.
# - Pendulum helps keep datetimes timezone-aware and consistent.
from pendulum import datetime


# Define the DAG via decorator configuration.
# Everything inside `@dag(...)` is *metadata/config* for scheduling and UI.
@dag(
    # When Airflow should create a new DAG Run.
    # "@daily" means a run per day (daily data interval).
    schedule="@daily",

    # Earliest date Airflow will consider when creating scheduled runs.
    # Note: this is not "run now". It is a scheduling boundary.
    start_date=datetime(2025, 1, 1),

    # Description shown in the Airflow UI.
    description="This DAG does...",

    # Tags help you filter/group DAGs in the UI.
    tags=["team_a", "source_a"],

    # Guardrail: if this many DAG runs fail consecutively, Airflow may auto-pause the DAG
    # (depending on Airflow version/config) to prevent repeated failures.
    max_consecutive_failed_dag_runs=3,
)

def my_dag_v1():
    """DAG factory function.

    Think of this function as the *blueprint builder*.

    When Airflow imports this file, it calls this function (via `my_dag()` at the bottom) to:
      - create task objects
      - define dependencies
      - assemble the DAG graph

    The real work of tasks runs later, when a DAG run is executed.
    """

    # Define a TaskFlow task.
    #
    # What is `@task`?
    # - It converts a normal Python function into an Airflow task definition.
    # - Airflow can schedule it, retry it, log it, and show it as a node in the UI.
    #
    # Important: The function body below does NOT run during DAG parsing.
    # It runs at task runtime, inside the executor/worker.
    @task
    def task_a():
        # Example business logic.
        # In Airflow, prints go to task logs.
        print("Hello from task A!")

    # Instantiate the task in the DAG.
    #
    # This line is the most confusing part for beginners:
    # - Calling `task_a()` here does NOT execute the print right now.
    # - Instead, it creates a task node (a scheduled step) in the DAG graph.
    #
    # Why?
    # - Because `@task` replaced the original function with an Airflow task wrapper.
    # - The call returns a handle (often an XComArg-like object) representing the task output.
    task_a()

    # If there were multiple tasks, you would define ordering/dependencies here.
    # Example:
    #   a = task_a()
    #   b = task_b(a)
    # TaskFlow uses function calls and returned handles to express data passing.


# Instantiate/register the DAG.
# - Airflow discovers DAGs by importing files in the DAGs folder.
# - Calling `my_dag()` produces the DAG object and registers it.
my_dag_v1()


"""Overview (what this code does end-to-end)

This file defines a daily Airflow workflow using TaskFlow. The `@dag(...)` decorator provides the DAG’s
scheduling and UI metadata (daily schedule, start date, description, tags, and a consecutive-failure
guardrail). Inside the `my_dag()` factory function, `@task` turns `task_a` into an Airflow task.
Calling `task_a()` inside the DAG function does not run the Python code immediately; it creates a task
node in the DAG graph. When Airflow later creates a DAG run (on schedule or manual trigger), the
scheduler queues the task, a worker executes it, and the `print` output appears in the task logs.

Key terminology (simple definitions)

- DAG: the workflow blueprint (tasks + dependencies + schedule/metadata).
- DAG run: one execution instance of the DAG for a specific time interval.
- Task: one step in the DAG. In the UI it is a node.
- @dag: a decorator that turns a function into a DAG factory.
- @task: a decorator that turns a function into a schedulable Airflow task.
- “Parsing”: Airflow importing the file to build the blueprint (no business work runs).
- “Runtime”: when workers actually execute task code during a DAG run.
"""