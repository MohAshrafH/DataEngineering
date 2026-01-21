# taskflow_branching_fanout.py
# Purpose: Define an Apache Airflow DAG using TaskFlow (Python-first style)
# This version matches the instructor screenshot EXACTLY:
#   a = task_a()
#   a >> [task_b(), task_c()]
#   a >> [task_d(), task_e()]

from airflow.sdk import dag, task
from pendulum import datetime


@dag(
    dag_id="taskflow_branching_fanout",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    description="TaskFlow fan-out branching example",
    tags=["team_a", "source_a"],
    max_consecutive_failed_dag_runs=3,
)
def my_dag():

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

    @task
    def task_e():
        print("Hello from task E!")

    # ---- Dependency wiring (as in screenshot) ----
    a = task_a()

    a >> task_b() >> task_c()
    a >> task_d() >> task_e()


# Register the DAG
my_dag()
