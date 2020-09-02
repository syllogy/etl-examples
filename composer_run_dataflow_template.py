import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator
from airflow.utils.dates import days_ago

bucket_path = models.Variable.get("gcs_bucket")
project_id = models.Variable.get("gcp_project")
gce_region = models.Variable.get("gce_region")


default_args = {
    "start_date": days_ago(1),
}

with models.DAG(
    "composer_dataflow_dag",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:

    start_template_job = DataFlowPythonOperator(
        task_id="dataflow_python",
        job_name="dataflow_python",
        py_file="/home/airflow/gcs/dags/dataflow/wordcount.py",
        dataflow_default_options = {
            "project": project_id,
            "runner": "DataflowRunner",
            "job_name": 'my_job',
            "staging_location": f'gs://{bucket_path}/staging', 
            "temp_location": f'gs://{bucket_path}/temp',
            "output": f'gs://{bucket_path}/output',
            "requirements_file": '/home/airflow/gcs/dags/dataflow/requirements.txt'
      }
    )
