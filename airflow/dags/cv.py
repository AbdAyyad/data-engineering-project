from datetime import timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator
import requests
import logging

import time

LOGGER = logging.getLogger("airflow.task")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
        'cv',
        default_args=default_args,
        description='ETL for data engineering project',
        schedule_interval=None,
        start_date=days_ago(2),
        tags=['ETL'],
) as dag:
    def upload_cv(**kwargs):
        ti = kwargs['ti']
        url = "http://185.96.69.234:8000/upload"
        payload = {}
        file_name = kwargs['dag_run'].conf['file_id']+'.pdf'
        files = [
            ('files', (file_name, open(kwargs['dag_run'].conf['path'], 'rb'), 'application/pdf'))
        ]
        headers = {
            'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
        }
        response = requests.request("POST", url, headers=headers, data=payload, files=files)
        LOGGER.info('status code from upload api: ' + str(response.status_code))
        LOGGER.info('response from upload api: ' + str(response.json()[0]))
        if response.status_code != 200:
            raise ValueError('upload api return status code' + str(response.status_code))
        ti.xcom_push(key='parser_id', value=response.json()[0])


    t1 = PythonOperator(
        task_id='upload_cv',
        python_callable=upload_cv,
        provide_context=True,
        dag=dag,
    )


    def download_parsed_data(**kwargs):
        ti = kwargs['ti']
        parser_id = ti.xcom_pull(key='parser_id', task_ids=['upload_cv'])
        time.sleep(5)
        LOGGER.info('document id: ' + str(parser_id))
        url = "http://185.96.69.234:8000/parsed?id=" + parser_id[0]

        payload = {}
        headers = {
            'X-API-KEY': '92f023f8b3ea67d455b9398c3f51fc48'
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        LOGGER.info('status code from download api: ' + str(response.status_code))
        LOGGER.info('response from download api: ' + str(response.json()))
        if response.status_code != 200:
            raise ValueError('download api return status code' + str(response.status_code))


    t2 = PythonOperator(
        task_id='download_parsed_data',
        python_callable=download_parsed_data,
        provide_context=True,
        dag=dag,
    )

    t1 >> t2
