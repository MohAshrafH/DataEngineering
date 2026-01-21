"""backfill_trigger_dag_airflow3.py

Goal
----
A controller DAG that triggers a *backfill-like* set of historical DAG runs
for another DAG in **Airflow 3**, without calling the Airflow CLI from inside
a task (which is blocked in Airflow 3 / Astro runtimes).

What this replaces
------------------
Instead of running:
    airflow dags backfill -s <start> -e <end> <dag_id>
inside a BashOperator, this DAG creates the same effect by programmatically
triggering one DAG run per logical date.

How it works (Flow)
-------------------
1) You manually trigger THIS DAG from the Airflow UI.
2) You pass a JSON `conf` payload with:
   - dag_id      : target DAG to backfill
   - date_start : first logical date (YYYY-MM-DD)
   - date_end   : last logical date (YYYY-MM-DD)
3) A Python task builds a list of logical dates between start and end.
4) TriggerDagRunOperator (mapped) triggers one DAG run per date.

This achieves the *idea of backfill* in an Airflow-3-compatible way.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List

# Airflow 3 TaskFlow-style imports
from airflow.sdk import dag, task

# Operator used to programmatically trigger DAG runs
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    # DAG id with required suffix
    dag_id="backfill_trigger_dag_3",

    # No automatic schedule; this DAG is triggered manually only
    schedule=None,

    # Anchor date: earliest logical date Airflow allows for this DAG
    start_date=datetime(2022, 1, 1),

    # Prevent automatic historical runs for THIS controller DAG
    catchup=False,

    # UI grouping / labeling
    tags=["backfill-trigger", "airflow3"],
)
def backfill_trigger_dag_3():

    @task
    def build_logical_dates() -> List[str]:
        """
        Build a list of logical dates between date_start and date_end (inclusive).

        The dates are returned as ISO-formatted strings so they can be safely
        stored in XCom and expanded over by mapped tasks.
        """
        # Access runtime context to read dag_run.conf
        from airflow.operators.python import get_current_context

        context = get_current_context()
        conf = context.get("dag_run").conf or {}

        # Read inputs provided when triggering THIS DAG
        date_start_str = conf["date_start"]
        date_end_str = conf["date_end"]

        # Convert strings to datetime objects (logical dates)
        start = datetime.fromisoformat(date_start_str)
        end = datetime.fromisoformat(date_end_str)

        if end < start:
            raise ValueError("date_end must be greater than or equal to date_start")

        # Build a daily list of logical dates
        logical_dates: List[str] = []
        current = start
        while current <= end:
            logical_dates.append(current.isoformat())
            current += timedelta(days=1)

        return logical_dates

    @task
    def get_target_dag_id() -> str:
        """
        Read the target DAG id from dag_run.conf.
        """
        from airflow.operators.python import get_current_context

        context = get_current_context()
        conf = context.get("dag_run").conf or {}
        return conf["dag_id"]

    # Build inputs for mapped triggering
    logical_dates = build_logical_dates()
    target_dag_id = get_target_dag_id()

    # Trigger one DAG run per logical date
    TriggerDagRunOperator.partial(
        # Task id inside THIS controller DAG
        task_id="trigger_backfill_3",

        # Reset existing DAG runs for the same logical date (similar to --reset-dagruns)
        reset_dag_run=True,

        # Fire-and-forget: do not wait for each triggered DAG to finish
        wait_for_completion=False,
    ).expand(
        # Target DAG to trigger
        trigger_dag_id=target_dag_id,

        # One logical_date per mapped task instance
        logical_date=logical_dates,
        # Optional configuration passed to each triggered DAG run
        # NOTE: `logical_dates` is an XComArg at parse time (not a real Python list),
        # so we cannot use `len(logical_dates)` here. If you need per-run conf,
        # build it in a separate @task that returns a list of dicts and expand over it.
    )


# Instantiate the DAG
backfill_trigger_dag_3()
