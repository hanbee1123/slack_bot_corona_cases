from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import sys

sys.path.append('/Users/ihanbi/Desktop/Projects/corona_count')
import crawler



default_arguments={
    'owner':'hbl',
    'email':'hanbee1123@gmail.com',
    'email_on_failure':True,
    'start_date':datetime(2020,5,28)
}
corona_count_dag = DAG(
    dag_id='corona_count_dag',
    default_args = default_arguments,
    schedule_interval = '*/30 * * * *',
    catchup = False
)

perform = BashOperator(
        task_id = 'crawl_and_upload',
        bash_command = 'python3 /Users/ihanbi/Desktop/Projects/corona_count/crawler.py',
        dag = corona_count_dag
        )

perform
