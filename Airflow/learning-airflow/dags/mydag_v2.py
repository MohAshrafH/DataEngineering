# my_dag.py
# Purpose: Define an Apache Airflow DAG using TaskFlow (the modern “Python-first” style):
#   - `@dag(...)` defines the workflow metadata (schedule, start date, tags, etc.)
#   - `@task` turns a Python function into an Airflow task
#
# This version matches the instructor screenshot by:
#   1) Defining four TaskFlow tasks (task_a, task_b, task_c, task_d)
#   2) Wiring dependencies exactly like:  task_a() >> task_b() >> [task_c(), task_d()]
#
# No classic Operators (e.g., PythonOperator) are used.


# Import the DAG decorator and TaskFlow task decorator.
#
# What is a decorator (in Python)?
# - A decorator wraps a function and returns a modified callable.
# - `@dag(...)` wraps a function so that calling it produces/registers an Airflow DAG.
# - `@task` wraps a function so that calling it produces/registers an Airflow Task.
from airflow.sdk import dag, task

# Pendulum datetime is recommended by Airflow.
# - Airflow scheduling expects timezone-aware datetimes.
from pendulum import datetime


@dag(
    # When Airflow should create a new DAG Run.
    # "@daily" means one run per day.
    schedule="@daily",

    # Earliest date Airflow will consider when scheduling.
    start_date=datetime(2025, 1, 1),

    # Shown in the Airflow UI.
    description="This DAG does...",

    # UI filtering/grouping.
    tags=["team_a", "source_a"],

    # Guardrail: after N consecutive failed DAG runs, Airflow may auto-pause (depends on config/version).
    max_consecutive_failed_dag_runs=3,
)

def my_dag():
    """DAG factory function.

    This function is executed during DAG *parsing* to build the workflow blueprint:
      - define tasks (nodes)
      - define dependencies (edges)

    The task bodies (the prints) are executed later at *runtime* during a DAG run.
    """

    # -----------------------------
    # Task definitions (TaskFlow)
    # -----------------------------
    # Each `@task` turns the function into an Airflow task definition.
    # The function body will run only when the task is executed by a worker.

    @task
    def task_a():
        print("Hello from task A!")

    @task
    def task_b():
        print("Hello from task B!")

    @task
    def task_c():
        print("Hello from task C!")

    @task
    def task_d():
        print("Hello from task D!")

    # -----------------------------
    # Dependency wiring (matches the screenshot)
    # -----------------------------

    # IMPORTANT: In TaskFlow, calling `task_x()` here does NOT run the print now.
    # It creates a task node in the DAG graph and returns a task object/handle.
    #
    # What does `>>` mean?
    # - `upstream >> downstream` means downstream runs AFTER upstream succeeds.
    #
    # What does the list mean?
    # - `x >> [y, z]` means BOTH y and z depend on x.
    # - y and z can then run in parallel (subject to executor/pool/concurrency limits).

    task_a() >> task_b() >> [task_c(), task_d()]


# Instantiate/register the DAG.
# Airflow discovers DAGs by importing Python files in the dags folder.
# Calling `my_dag()` produces the DAG object and registers it.
my_dag()


"""Overview (what this code does end-to-end)

This file defines a daily Airflow DAG using TaskFlow. The `@dag(...)` decorator provides scheduling and UI
metadata. Inside the DAG factory, four tasks are defined with `@task`. The final dependency line
`task_a() >> task_b() >> [task_c(), task_d()]` builds a simple graph:

- task_a runs first
- task_b runs after task_a succeeds
- task_c and task_d both run after task_b succeeds (parallel branch)

During parsing, Airflow imports the file and constructs the DAG graph; the print statements do not execute.
During runtime (a DAG run), the scheduler queues the tasks and workers execute them; the print outputs appear
in each task’s log.

Key terminology (simple definitions)

- DAG: the workflow blueprint (tasks + dependencies + schedule/metadata).
- DAG run: one execution instance of the DAG for a specific time interval.
- Task: one step/node in the DAG.
- @dag: decorator that turns a function into a DAG factory.
- @task: decorator that turns a function into a schedulable Airflow task.
- Parsing: Airflow importing the file to build the blueprint (no business work runs).
- Runtime: when workers actually execute task code during a DAG run.
- `>>`: dependency operator meaning “runs before”.
- `[task_c(), task_d()]`: branching meaning “both depend on task_b; run in parallel after task_b”.
"""
