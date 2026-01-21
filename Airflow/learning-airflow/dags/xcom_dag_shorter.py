# Import decorators used to define DAGs and tasks.
# This version is a *shorter* form of the previous example.
# The main difference is how the Task Instance (ti) is received.
from airflow.sdk import dag, task


# Define the DAG container.
# Same DAG purpose as before: demonstrate XCom push and pull.
@dag
def xcom_dag_shorter():

    # SHORTER DIFFERENCE #1:
    # Instead of **context: Context, we directly receive `ti` as a parameter.
    # Airflow automatically injects the Task Instance when it sees `ti`.
    @task
    def task_a(ti):
        # Create a local Python variable.
        # As before, this value exists only inside task_a.
        val = 42

        # Push the value into XCom.
        # Same behavior as the longer version:
        # - key identifies the value
        # - value is stored in Airflow's XCom backend
        ti.xcom_push(key="my_key", value=val)


    # Define the downstream task.
    @task
    def task_b(ti):
        # SHORTER DIFFERENCE #2:
        # We directly use `ti` instead of context['ti'].
        # Logic and result are identical.
        val = ti.xcom_pull(task_ids="task_a", key="my_key")

        # Print the retrieved XCom value.
        # Expected output in logs: 42
        print(val)


    # Define task execution order.
    # task_a must complete before task_b runs.
    task_a() >> task_b()


# Instantiate the DAG so Airflow can discover it.
xcom_dag_shorter()
