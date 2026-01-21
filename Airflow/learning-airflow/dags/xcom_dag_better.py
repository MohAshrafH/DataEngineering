# This is the *better / cleanest* version of the XCom example.
# It uses pure TaskFlow semantics where return values are passed between tasks.
# Airflow still uses XCom internally, but this version hides XCom details
# and lets you write code that looks like normal Python.

from airflow.sdk import dag, task


# Define the DAG container.
# Same workflow goal as previous versions:
# task_a produces a value, task_b consumes it.
@dag
def xcom_dag_better():

    # Define the first task.
    # No context, no ti, no explicit XCom calls.
    # The returned value is automatically stored in XCom by Airflow.
    @task
    def task_a():
        # Create a simple Python value.
        val = 42

        # Returning a value from a TaskFlow task automatically:
        # - pushes the value to XCom
        # - associates it with this task and this DAG run
        return val


    # Define the downstream task.
    # The parameter `val` is not a normal Python argument at parse time.
    # It represents the XCom output of task_a.
    @task
    def task_b(val: int):
        # Airflow resolves `val` at runtime by pulling it from XCom.
        # From the developer's perspective, it behaves like a normal variable.
        print(val)


    # Define task dependencies using function calls.
    # task_a() returns a proxy object, not the actual value 42 at parse time.
    # task_b(val) wires task_b to consume the output of task_a.
    val = task_a()
    task_b(val)


# Instantiate the DAG so Airflow can register it.
xcom_dag_better()
