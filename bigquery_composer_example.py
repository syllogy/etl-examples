import datetime

from airflow import models
from airflow.contrib.operators import bigquery_get_data
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators import bigquery_to_gcs
from airflow.operators import bash_operator
from airflow.operators import email_operator
from airflow.utils import trigger_rule

# BigQuery
bq_dataset_name = models.Variable.get('dataset')
bq_table_name = models.Variable.get('table')
bq_output = f'{bq_dataset_name}.{bq_table_name}'

# GCS
gcs_bucket=models.Variable.get('gcs_bucket')
output_file = f'gs://{gcs_bucket}/output.csv'

# Start Time
start_date = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# [START composer_notify_failure]
default_dag_args = {
    'start_date': start_date,
    'email': models.Variable.get('email'),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'project_id': models.Variable.get('gcp_project')
}

with models.DAG(
        'bq_example',
        max_active_runs=1,
        schedule_interval=datetime.timedelta(days=1),
        default_args=default_dag_args) as dag:


    bq_query = bigquery_operator.BigQueryOperator(
        task_id='bq_query',
        bql="""
        SELECT *
        FROM `david-playground-1.orderedtest.unordered`
        """,
        use_legacy_sql=False,
        write_disposition='WRITE_TRUNCATE',
        destination_dataset_table=bq_output)

    export_to_gcs = bigquery_to_gcs.BigQueryToCloudStorageOperator(
        task_id='export_to_gcs',
        source_project_dataset_table=bq_output,
        destination_cloud_storage_uris=[output_file],
        export_format='CSV')


    email_summary = email_operator.EmailOperator(
        task_id='email_summary',
        to=models.Variable.get('email'),
        subject='Sample BigQuery data ready',
        html_content="""
        Ran Job at {start_date}
        Export available at {export_location}
        """.format(
            start_date=start_date,
            export_location=output_file))

    # Define DAG dependencies.
    bq_query >> export_to_gcs >> email_summary
