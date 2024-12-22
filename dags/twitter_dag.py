# from datetime import datetime

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.bash import BashOperator

# # A DAG represents a workflow, a collection of tasks
# with DAG(dag_id="twitter_elt_pipeline", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:
#     # Tasks are represented as operators
#     list =BashOperator(task_id="list", bash_command="ls")
#     fetch_tweets = BashOperator(
#         task_id='fetch_tweets',
#         bash_command='python /usr/local/airflow/dags/script/fetch_tweets.py', 
#     )

#     @task()
#     def airflow():
#         print("Completed Scraping...")

#     # Set dependencies between tasks
#     list >> fetch_tweets >> airflow()