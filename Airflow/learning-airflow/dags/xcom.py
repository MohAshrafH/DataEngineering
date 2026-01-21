# Import Airflow SDK helpers for building DAGs and tasks.
# - dag: decorator that turns a function into a DAG definition.
# - task: decorator that turns a Python function into an Airflow task.
# - Context: a type hint describing the "context" dictionary Airflow passes at runtime.
from airflow.sdk import dag, task, Context


# Define a DAG using the @dag decorator.
# A DAG is the workflow container: it holds tasks and their ordering/dependencies.
# In TaskFlow, the function body is where you define tasks and connect them.
@dag
def xcom_dag():

    # Define task_a as an Airflow task.
    # @task converts this Python function into a schedulable unit in Airflow.
    # The **context: Context part means:
    #   - This function accepts keyword arguments (**) as a dictionary.
    #   - Airflow will provide runtime metadata in that dictionary.
    #   - We type-hint it as Context for readability and editor help.
    @task
    def task_a(**context: Context):
        # Create a normal Python variable.
        # IMPORTANT: this variable exists only inside this task's execution process.
        # It is not automatically visible to other tasks.
        val = 42

        # context['ti'] accesses the Task Instance object for this run.
        # Task Instance (ti) represents "this task, in this specific DAG run".
        # xcom_push stores a small value in Airflow's XCom storage.
        #
        # key='my_key':
        #   - a label/name so we can retrieve the correct value later.
        # value=val:
        #   - the data we want to store (here: 42).
        #
        # The pushed XCom is scoped to:
        #   dag_id + run_id + task_id (task_a) + key ('my_key')
        # so it won’t collide with other runs or other tasks.
        context['ti'].xcom_push(key='my_key', value=val)


    # Define task_b as another Airflow task.
    # It will pull the value stored by task_a.
    @task
    def task_b(**context: Context):
        # Pull an XCom value from Airflow's storage.
        #
        # task_ids='task_a':
        #   - tells Airflow "get the value produced by the task with id task_a".
        #
        # key='my_key':
        #   - tells Airflow "get the value stored under this label".
        #
        # The combination (task_ids + key) in the current DAG run identifies the exact record.
        val = context['ti'].xcom_pull(task_ids='task_a', key='my_key')

        # Print the pulled value.
        # In logs, you should see: 42
        print(val)


    # Set dependency order.
    # task_a() and task_b() here do NOT execute immediately like normal Python.
    # Instead, calling them registers tasks in the DAG definition and returns task objects.
    #
    # The >> operator means "task_a must finish before task_b starts".
    task_a() >> task_b()


# Instantiate the DAG.
# This last line is critical: it makes Airflow discover a DAG object when it imports the file.
# Without it, the DAG definition might not be registered.
xcom_dag()
