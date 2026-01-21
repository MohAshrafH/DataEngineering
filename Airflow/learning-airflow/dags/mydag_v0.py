# my_dag.py
# Purpose: Define an Apache Airflow DAG (workflow) using the @dag decorator.
# A DAG is a *definition* of a workflow (tasks + dependencies + scheduling metadata).

# Import the DAG decorator from Airflow's SDK.
# - The decorator lets you define DAG metadata (schedule, start_date, tags, etc.)
#   and wrap a Python function whose body will declare tasks and dependencies.
# - Using `@dag(...)` is part of Airflow's TaskFlow-style authoring pattern.
from airflow.sdk import dag

# Import Pendulum's datetime constructor.
# - Airflow expects timezone-aware datetimes.
# - `pendulum.datetime(...)` produces a Pendulum DateTime (recommended by Airflow)
# and avoids common timezone pitfalls compared to naive Python `datetime`.
from pendulum import datetime

# Define a DAG via a decorator.
# Everything inside the parentheses is *DAG configuration* (metadata), not execution logic.
# Airflow reads this file, evaluates it, and registers the DAG in the UI.
@dag(
    # The schedule tells Airflow when to create a new DAG Run.
    # - "@daily" is a cron preset meaning: run once per day.
    # - Important: the schedule creates runs for *data intervals*; it does not mean
    # "run immediately every 24h"—it means "create a run for each daily interval".
    schedule = "@daily",

    # The earliest date/time from which Airflow is allowed to schedule runs.
    # - With schedule="@daily", Airflow will consider daily intervals starting at this date.
    # - This is not "run now". Airflow backfills runs from this point depending on settings.
    # - `datetime(2025, 1, 1)` is 2025-01-01 00:00:00 (Pendulum DateTime).
    start_date = datetime(2025,1,1),

    # A free-text description shown in the Airflow UI.
    # Use this to document what the workflow does at a high level.
    description = "This DAG does bla bla bla...",

    tags = ["team_a","source_a"],

    max_consecutive_failed_dag_runs = 3
)

def my_dag_v0():
    """Declare tasks and dependencies for the workflow.

    In a real DAG, you'd place task definitions here, for example:
      - Extract data from a source
      - Transform/validate the dataset
      - Load results to a warehouse

    This example uses `pass`, so the DAG contains no tasks and therefore does nothing.
    """

    # `pass` is a Python no-op (placeholder).
    # It keeps the function syntactically valid while you build out tasks later.
    pass

# Instantiate/register the DAG so Airflow can discover it.
my_dag_v0()

"""Overview

This file defines an Airflow workflow using the `@dag` decorator. The decorator supplies scheduling
and metadata (daily cadence, start date, UI description, tags, and an auto-pause guardrail after three
consecutive failed runs). The decorated `my_dag()` function is the place where tasks and dependencies
would normally be declared, but in this snippet it contains only `pass`, so the DAG registers in Airflow
but executes no tasks. In practice, you would replace `pass` with TaskFlow `@task` functions or operator
instances, then set their execution order with dependency operators (e.g., `task_a >> task_b`).
"""
