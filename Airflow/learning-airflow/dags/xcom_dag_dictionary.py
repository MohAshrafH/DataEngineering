# This file defines an Airflow DAG that demonstrates using XCom
# to pass a *dictionary* (key-value structure) between tasks.
#
# OVERVIEW (what this DAG does):
# 1) task_a builds a dictionary containing multiple related values.
# 2) task_a pushes the entire dictionary into XCom under a single key.
# 3) task_b pulls that dictionary from XCom and prints it.
#
# Why this pattern is recommended:
# - When multiple values logically belong together, storing them as one dictionary
#   is cleaner than pushing many separate XCom keys.
# - Downstream tasks receive one structured object instead of many loose values.
# - This improves readability and reduces XCom management complexity.

from airflow.sdk import dag, task


# @dag marks this function as an Airflow DAG definition.
# The DAG groups tasks and defines their execution order.
@dag
def xcom_dag_dictionary():

    # First task: produces a dictionary and stores it in XCom.
    @task
    def task_a(ti):
        # Create a Python dictionary.
        # This dictionary groups multiple related values together.
        val = {
            "val_1": 42,
            "val_2": 43,
        }

        # Push the dictionary into XCom.
        # key="my_key" identifies this XCom entry.
        # value=val stores the entire dictionary as a single object.
        # Airflow serializes this dictionary so it can be stored and retrieved safely.
        ti.xcom_push(key="my_key", value=val)


    # Downstream task: retrieves the dictionary from XCom.
    @task
    def task_b(ti):
        # Pull the dictionary from XCom.
        # task_ids=["task_a"] specifies the task that produced the value.
        # key="my_key" specifies which XCom entry to retrieve.
        #
        # Because only one task_id is provided, the result is a single object
        # (not a list). The returned value is the dictionary pushed by task_a.
        vals = ti.xcom_pull(task_ids=["task_a"], key="my_key")

        # Print the pulled dictionary.
        # Expected output in logs:
        # {'val_1': 42, 'val_2': 43}
        print(vals)


    # Define task execution order.
    # task_b must run after task_a so the XCom value exists.
    task_a() >> task_b()


# Instantiate the DAG so Airflow can discover it.
xcom_dag_dictionary()
