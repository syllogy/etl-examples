import datetime
from airflow import models
from airflow.operators import bash_operator

with models.DAG(
       'composer_hello_world',
       schedule_interval=datetime.timedelta(days=1),
       default_args={'start_date': datetime.datetime(2020, 9, 1),}) as dag:

   goodbye_bash = bash_operator.BashOperator(
       task_id='hello_world',
       bash_command='echo “Hello World”')
