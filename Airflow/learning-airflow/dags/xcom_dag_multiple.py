# This file defines an Airflow DAG that demonstrates *multiple XCom pulls*.
#
# OVERVIEW (what this DAG does):
# 1) task_a produces the value 42 and pushes it to XCom under the key "my_key".
# 2) task_c produces the value 43 and pushes it to XCom under the same key "my_key".
# 3) task_b pulls *both* XCom values (from task_a and task_c) using a single xcom_pull call,
#    then prints the list of pulled values.
#
# Why this is useful:
# - In real pipelines, a downstream task often needs results from *multiple upstream tasks*
#   (example: row counts from several loads, file paths from several extracts, etc.).
# - XCom lets you store small values per task, and pull them later in a controlled way.

from airflow.sdk import dag, task


# @dag turns the function below into an Airflow DAG definition.
# The DAG is a container for tasks plus their dependency relationships.
@dag
def xcom_dag_multiple():

    # @task turns a normal Python function into an Airflow task.
    # `ti` means "Task Instance" for the current execution.
    # Airflow injects `ti` at runtime so you can interact with XCom.
    @task
    def task_a(ti):
        # Create a local Python variable with the value we want to share.
        val = 42

        # Push this value into XCom.
        # key="my_key" is the label used to identify the stored value.
        # value=val is the data to store.
        # This XCom is automatically tied to:
        # - this DAG run (so runs don't mix)
        # - this task instance (task_a)
        ti.xcom_push(key="my_key", value=val)


    # A second upstream task that also pushes a value under the same key.
    # Using the same key is fine because the producing task_id is different.
    @task
    def task_c(ti):
        # Create a different value so we can tell the two sources apart.
        val = 43

        # Push to XCom under the same key.
        # This produces a separate XCom record because it's from task_c.
        ti.xcom_push(key="my_key", value=val)


    # Downstream task that needs values from multiple upstream tasks.
    @task
    def task_b(ti):
        # Pull XComs from *multiple* tasks in one call.
        # task_ids=["task_a", "task_c"] means:
        #   - fetch the XCom value produced by task_a
        #   - fetch the XCom value produced by task_c
        # key="my_key" means:
        #   - from each of those tasks, retrieve the XCom stored under this key
        #
        # Because task_ids is a list, xcom_pull returns a list in the same order.
        vals = ti.xcom_pull(task_ids=["task_a", "task_c"], key="my_key")

        # Print the list of values pulled from XCom.
        # Expected output in task_b logs: [42, 43]
        print(vals)


    # Define execution order.
    # This creates dependencies so task_b runs *after* both task_a and task_c.
    # The chain shown is: task_a -> task_c -> task_b.
    #
    # IMPORTANT detail:
    # - With this exact chain, task_c depends on task_a (it runs after task_a).
    # - task_b depends on task_c (it runs after task_c).
    # - Therefore task_b is guaranteed to run after *both* tasks.
    task_a() >> task_c() >> task_b()


# Instantiate the DAG object so Airflow can discover it when importing this file.
xcom_dag_multiple()
