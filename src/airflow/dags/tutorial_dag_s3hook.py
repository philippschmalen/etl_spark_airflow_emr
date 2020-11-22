import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago 
from airflow.hooks.S3_hook import S3Hook


def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    # prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}")#/{prefix}
    keys = hook.list_keys(bucket) #, prefix=prefix
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")


dag = DAG(
        'lesson1.exercise4_S3hooks',
        start_date=days_ago(2))

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
)