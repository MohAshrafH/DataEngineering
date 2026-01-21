# taskflow_chain_helper.py
#
# Purpose
# -------
# Define an Apache Airflow DAG using TaskFlow (the modern “Python-first” style) AND use
# Airflow's *helper* function `chain(...)` to set dependencies in a clean, readable way.
#
# This file demonstrates the instructor-style pattern:
#     chain(task_a(), [task_b(), task_d()], [task_c(), task_e()])
#
# Which conceptually means:
#   1) Run task_a first
#   2) Then run task_b and task_d (in parallel)
#   3) Then run task_c and task_e (in parallel)
#
# Put differently, it creates edges like:
#   task_a -> task_b -> task_c
#   task_a -> task_b -> task_e
#   task_a -> task_d -> task_c
#   task_a -> task_d -> task_e
#
# The key idea: `chain` is a *dependency builder*. It does NOT execute tasks.


# Import TaskFlow decorators and dependency helpers from Airflow's SDK.
#
# - `@dag` wraps a function and turns it into a DAG factory (a blueprint builder).
# - `@task` wraps a Python function and turns it into a schedulable Airflow task.
# - `chain` is a helper function that sets dependencies across tasks (including lists of tasks)
#   without writing many `>>` lines.
from airflow.sdk import dag, task, chain

# Import timezone-aware datetime.
# Airflow schedules runs based on time intervals, so timezone-aware datetimes avoid confusion.
from pendulum import datetime


@dag(
    # Explicit DAG ID (the name you see in the Airflow UI list).
    # If you do not set this, Airflow may derive an ID from the function name.
    dag_id="taskflow_chain_helper",

    # Create a DAG run once per day.
    schedule="@daily",

    # Earliest date from which Airflow is allowed to create scheduled runs.
    start_date=datetime(2025, 1, 1),

    # UI metadata (helps you recognize the DAG in the UI).
    description="TaskFlow example using chain() helper for dependencies",
    tags=["team_a", "source_a"],

    # Safety/guardrail: after this many consecutive failed DAG runs, Airflow may auto-pause
    # the DAG (behavior depends on Airflow version/config).
    max_consecutive_failed_dag_runs=3,
)
def my_dag():
    """DAG factory function.

    Parsing vs runtime
    ------------------
    - *Parsing time*: Airflow imports this file and executes `my_dag()` to build the DAG graph.
      It creates task objects and dependency edges.
    - *Runtime*: When a DAG run happens, Airflow schedules the tasks; workers execute task code.

    Important:
    - The `print(...)` calls below run only at task runtime, not at parse time.
    """

    # -----------------------------
    # Task definitions (TaskFlow)
    # -----------------------------

    @task
    def task_a():
        # This code runs in the task's execution environment (worker) during a DAG run.
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

    @task
    def task_e():
        print("Hello from task E!")

    # ----------------------------------------
    # Dependency wiring using chain(...)
    # ----------------------------------------

    # What does calling `task_x()` mean in TaskFlow?
    # - It does NOT run the Python function immediately.
    # - It creates a task node in the DAG graph and returns a *task handle* (an object that
    #   Airflow uses to represent the task in dependency expressions).
    #
    # Why do we call them here at parse time?
    # - Because we are constructing the DAG graph (nodes + edges).

    # `chain(...)` is a helper that connects items left-to-right.
    #
    # Rules of `chain` (practical mental model):
    # - `chain(A, B, C)` behaves like: A >> B >> C
    # - If an argument is a list, it represents a *group* of tasks at that level.
    # - When it sees list-to-list, it builds a *cross product* of dependencies:
    #     every upstream task in the left list becomes upstream of every downstream task
    #     in the right list.
    #
    # So here:
    #   chain(task_a(), [task_b(), task_d()], [task_c(), task_e()])
    # means:
    #   - task_a is upstream of BOTH task_b and task_d
    #   - task_b and task_d are each upstream of BOTH task_c and task_e
    chain(task_a(), [task_b(), task_d()], [task_c(), task_e()])

    # Equivalent dependency wiring (same graph), written without helpers:
    #
    # a = task_a()
    # a >> [task_b(), task_d()]
    # task_b() >> [task_c(), task_e()]
    # task_d() >> [task_c(), task_e()]
    #
    # Note: the exact equivalent requires either multiple lines or careful reuse of handles.


# Register the DAG (this must run at import time so Airflow can discover it).
my_dag()


"""Overview (what this code does end-to-end)

File: taskflow_chain_helper.py

This file defines a daily TaskFlow DAG named `taskflow_chain_helper`. Inside the DAG factory function,
we define five TaskFlow tasks (task_a through task_e). We then use the Airflow helper `chain(...)` to
create dependencies in one readable line:

    chain(task_a(), [task_b(), task_d()], [task_c(), task_e()])

This builds a three-level workflow:

- Level 1: task_a runs first
- Level 2: task_b and task_d run after task_a (parallel fan-out)
- Level 3: task_c and task_e run after BOTH task_b and task_d (a join-style requirement)

Key point: `chain` is only a graph-construction helper during parsing; it does not run tasks.

What are “helpers” here?

- `chain(...)`: convenience function to set many dependencies at once, including lists.
- `>>` / `<<`: dependency operators (syntactic sugar) meaning “runs before / runs after”.
- Lists in dependencies (e.g., `x >> [y, z]`): a compact way to express fan-out/fan-in.
- `chain_linear(...)` (if available in your environment): a stricter helper used when you want
  a purely linear chain without cross-product behavior.
"""
