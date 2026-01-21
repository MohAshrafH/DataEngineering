"""backfill_trigger_dag_commented.py

OVERVIEW (What this DAG is and why it exists)
---------------------------------------------
This file defines an Apache Airflow DAG whose *single job* is to run an Airflow CLI command that
triggers a *backfill* for another DAG.

- A "backfill" means: "run a DAG for a range of past logical dates" (historical dates).
- This DAG does NOT process data itself. It acts like a "controller" (a launcher).
- You trigger this DAG manually from the Airflow UI and pass parameters (conf) such as:
    - date_start: the first logical date to backfill
    - date_end:   the last logical date to backfill
    - dag_id:     the target DAG to backfill

FLOW (Step-by-step)
-------------------
1) You open Airflow UI and manually trigger this DAG.
2) While triggering, you provide a JSON "conf" payload, e.g.:
   {
     "date_start": "2023-01-01",
     "date_end":   "2023-01-07",
     "dag_id":     "my_target_dag"
   }
3) The only task "trigger_backfill" runs a Bash command.
4) That Bash command calls:
     airflow dags backfill ...
   which creates/runs the historical DAG runs for the target DAG.

IMPORTANT NOTES (Practical behavior)
-----------------------------------
- This DAG is set to schedule_interval=None, so it will *not* run on a timetable.
  It runs only when you trigger it manually.
- catchup=False on THIS DAG only prevents THIS DAG from auto-creating old runs.
  It does NOT stop the backfill command from creating old runs for the target DAG.
- The task is a BashOperator, so it executes the CLI inside the Airflow worker/container.
  That means the container must have access to the Airflow CLI and metadata DB.

"""

# Import DAG: the core Airflow object that defines a workflow (a directed graph of tasks).
from airflow import DAG

# Import BashOperator: a built-in operator that runs a shell (bash) command.
from airflow.operators.bash import BashOperator

# Import datetime: used to define the DAG's start_date (the earliest logical date Airflow uses).
from datetime import datetime


# Create the DAG using a context manager (the "with" block).
# Everything inside this block becomes part of the DAG definition.
with DAG(
    # dag_id is the unique identifier for this DAG in the Airflow UI/DB.
    dag_id='backfill_trigger_dag',

    # schedule_interval=None means: no automatic scheduling.
    # This DAG will run only when you trigger it manually (UI or CLI).
    schedule=None,

    # start_date is the earliest logical date Airflow considers for scheduling.
    # For a manually-triggered DAG, this mainly documents the earliest intended date.
    # It also matters if someone later adds a schedule.
    start_date=datetime(2022, 1, 1),

    # tags are labels used in the Airflow UI to group/search DAGs.
    tags=['backfill-trigger-cli'],

    # catchup=False means: if this DAG had a schedule, Airflow would NOT create
    # missed historical runs automatically.
    # Since schedule_interval=None, it mostly serves as a safety setting.
    catchup=False
) as dag:

    # This task will run an Airflow CLI command to backfill another DAG.
    # The idea is: you pass configuration (conf) when triggering this DAG,
    # and the task reads those values and constructs the backfill command.

    trigger_backfill = BashOperator(
        # task_id is the unique name of the task inside this DAG.
        task_id='trigger_backfill',

        # bash_command is the exact shell command to execute.
        # It uses Airflow templating syntax {{ ... }} to inject runtime values.
        #
        # Explanation of the command pieces:
        # - airflow dags backfill
        #     Airflow CLI command that runs a DAG for a date range (historical logical dates).
        #
        # - --reset-dagruns
        #     If runs for those dates already exist, reset them so backfill can re-run.
        #     (This can overwrite previous run states for that date range.)
        #
        # - -y
        #     Auto-confirm "yes" to prompts, so the command can run non-interactively.
        #
        # - -s <start>
        #     Start logical date for the backfill.
        #
        # - -e <end>
        #     End logical date for the backfill.
        #
        # - <dag_id>
        #     The target DAG identifier to backfill.
        #
        # Where do <start>, <end>, and <dag_id> come from?
        # - dag_run.conf is the JSON configuration you pass when triggering THIS DAG.
        # - Example conf keys used below:
        #     dag_run.conf['date_start']  -> becomes the -s value
        #     dag_run.conf['date_end']    -> becomes the -e value
        #     dag_run.conf['dag_id']      -> becomes the target DAG id
        #
        # Example rendered command after templating might look like:
        # airflow dags backfill --reset-dagruns -y -s 2023-01-01 -e 2023-01-07 my_target_dag
        bash_command=(
            "airflow dags backfill --reset-dagruns -y "
            "-s {{ dag_run.conf['date_start'] }} "
            "-e {{ dag_run.conf['date_end'] }} "
            "{{ dag_run.conf['dag_id'] }}"
        )
    )

    # Placing the task variable at the end ensures it is registered in the DAG.
    # (In Airflow, just creating the operator inside the DAG context is enough,
    #  but this line makes it explicit that the DAG has a task.)
        # This line does NOT execute the task.
    # `trigger_backfill` is already a BashOperator object created above.
    # In operator-based DAGs, tasks are objects (not functions),
    # so they are NOT called with parentheses.
    # Airflow executes this task later via the scheduler/worker,
    # not at DAG parse time.
    trigger_backfill
