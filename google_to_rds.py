__author__ = 'Ayomide Aro'

import json
import boto3

from airflow import DAG

from datetime import timedelta
from datetime import datetime
import time

from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 4, 6),
    'email': ['chris@harwardmedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'wait_for_downstream': True,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG('google_to_rds', catchup=False, default_args=default_args, schedule_interval="0 * * * *")

aws_conn = BaseHook.get_connection("aws_conn")


def trigger_lambda_rds_update(**kwargs):
    payload = json.dumps({'type': 'google'})
    lambda_client = boto3.client(
        'lambda',
        aws_access_key_id=aws_conn.extra_dejson['aws_access_key_id'],
        aws_secret_access_key=aws_conn.extra_dejson['aws_secret_access_key'],
        region_name='us-west-2'
    )
    response_from_lambda = lambda_client.invoke(
        FunctionName='replaceFacebookDataInRds',
        InvocationType='Event',
        LogType='Tail',
        Payload=payload
    )

    print("Response from lambda ", response_from_lambda)

    return True


def trigger_google_glue_job(**kwargs):
    s3 = boto3.resource('s3')
    _config_arr = []
    s3_client = boto3.client('s3')
    configs = s3_client.list_objects(
        Bucket='harward-lake',
        Delimiter='/',
        Prefix='configuration/google/'
    )

    for con in configs['Contents']:
        if con['Key'] != 'configuration/google/':
            data = s3.Object('harward-lake', con['Key'])
            message = str(data.get()['Body'].read(), 'utf-8')
            _config_arr.append(json.loads(message))

    client = boto3.client(service_name='glue', region_name='us-west-2',
                          endpoint_url='https://glue.us-west-2.amazonaws.com')

    response = client.start_job_run(
        JobName='GoogleSpendToRds', Arguments={
            '--CONFIGS': json.dumps(_config_arr)})

    is_running = True
    while is_running:
        status = client.get_job_run(
            JobName='GoogleSpendToRds', RunId=response['JobRunId'])
        if status['JobRun']['JobRunState'] == 'SUCCEEDED':
            print("SUCCEEDED")
            is_running = False
            break
        elif status['JobRun']['JobRunState'] == 'FAILED':
            print("Job failed ")
            is_running = False
            break
        else:
            time.sleep(200)
    return True


def trigger_google_crawler(**kwargs):
    client = boto3.client(service_name='glue', region_name='us-west-2',
                          endpoint_url='https://glue.us-west-2.amazonaws.com')

    response = client.start_crawler(
        Name='googlespendcrawler'
    )

    print("Response from crawler >>>>>>>>> ", response)

    time.sleep(60 * 7)
    return True


t3 = PythonOperator(
    task_id='trigger_lambda_rds_update',
    provide_context=True,
    python_callable=trigger_lambda_rds_update,
    dag=dag)

t2 = PythonOperator(
    task_id='trigger_google_glue_job',
    provide_context=True,
    python_callable=trigger_google_glue_job,
    dag=dag)

t1 = PythonOperator(
    task_id='trigger_google_crawler',
    python_callable=trigger_google_crawler,
    provide_context=True,
    dag=dag)

t1 >> t2 >> t3
