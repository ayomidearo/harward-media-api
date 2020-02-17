__author__ = 'Ayomide Aro'

import json
import stripe
import redis
import os
import requests

from airflow import DAG
from airflow.models import Variable

from datetime import timedelta
from datetime import datetime, date
import time

from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook

from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': ['admin@harwardmedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'wait_for_downstream': True,
    'retry_delay': timedelta(minutes=3),
}

dag_name = "DAG_NAME"
dag = DAG(dag_name, catchup=False, default_args=default_args, schedule_interval="SCHEDULE_INTERVAL")

dag_config = Variable.get('VARIABLES_NAME', deserialize_json=True)
klarna_username = dag_config['klarna_username']
klarna_password = dag_config['klarna_password']
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
account_name = dag_config['account_name']
file_path = dag_config['file_path']
s3_folder_path = "/{datasource_type}/{transaction_type}/{account_name}/{year}/{month}/{day}/"

file_key_regex = 'klarna_transaction_{0}/'.format(account_name)

print("Starting job rn ")

timestamp = int(time.time() * 1000.0)

BASE_URL = "https://api-na.klarna.com"

transaction_url = BASE_URL + "/settlements/v1/transactions"


def confirm_parameters(**kwargs):
    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    print(dag_config)
    return True


def get_insight_from_klarna(ds, **kwargs):
    r = redis.Redis(host='redis', port=6379, db=2)

    session = requests.Session()
    session.auth = (klarna_username, klarna_password)

    if r.exists(dag_name) == 1:
        transactions = []
        response = session.get(transaction_url + "?offset={offset}".format(offset=int(r.hget(dag_name, "offset"))))
        insight_response_body = response.json()

        print("Subsequent transaction body ", insight_response_body)

        pagination = insight_response_body['pagination']
        total = pagination['total']

        if len(insight_response_body['transactions']) > 0:
            count = 0

            while 'next' in insight_response_body.get('pagination', {}):
                time.sleep(2)
                transactions.extend(insight_response_body['transactions'])
                response = session.get(insight_response_body['pagination']['next'])
                print("Next url count {count} ".format(count=count), insight_response_body['pagination'])
                response.raise_for_status()
                insight_response_body = response.json()
                count = count + 1

            transactions.extend(insight_response_body['transactions'])

            r.hset(dag_name, "offset", total)

            session.close()

            return transactions
        else:
            return transactions
    else:
        transactions = []
        response = session.get(transaction_url)
        insight_response_body = response.json()

        print("Initial transaction body ", insight_response_body)

        pagination = insight_response_body['pagination']
        total = pagination['total']

        if total > 0:
            count = 0

            while 'next' in insight_response_body.get('pagination', {}):
                time.sleep(2)
                transactions.extend(insight_response_body['transactions'])
                response = session.get(insight_response_body['pagination']['next'])
                print("Next url count {count} ".format(count=count), insight_response_body['pagination'])
                response.raise_for_status()
                insight_response_body = response.json()
                count = count + 1

            transactions.extend(insight_response_body['transactions'])

            r.hset(dag_name, "offset", total)

            return transactions
        else:
            return transactions


def write_transaction_to_file(**kwargs):
    charge_filename = 'sale_data_{0}.json'.format(timestamp)
    refund_filename = 'return_data_{0}.json'.format(timestamp)
    dispute_filename = 'reversal_data_{0}.json'.format(timestamp)
    fee_filename = 'fee_data_{0}.json'.format(timestamp)
    charge_f = open(file_path + file_key_regex + charge_filename, 'w+')
    refund_f = open(file_path + file_key_regex + refund_filename, 'w+')
    dispute_f = open(file_path + file_key_regex + dispute_filename, 'w+')
    fee_f = open(file_path + file_key_regex + fee_filename, 'w+')

    trxs = kwargs['ti'].xcom_pull(task_ids='get_insight_from_klarna')
    for trx in trxs:
        if trx['type'] == "SALE":
            charge_f.write(json.dumps(trx) + "\n")
        elif trx['type'] == "RETURN":
            refund_f.write(json.dumps(trx) + "\n")
        elif trx['type'] == "REVERSAL":
            dispute_f.write(json.dumps(trx) + "\n")
        elif trx['type'] == "FEE":
            fee_f.write(json.dumps(trx) + "\n")

    charge_f.close()
    refund_f.close()
    dispute_f.close()
    fee_f.close()

    return_data = [
        {"type": "sale", "path": file_path + file_key_regex + charge_filename, "filename": charge_filename},
        {"type": "return", "path": file_path + file_key_regex + refund_filename, "filename": refund_filename},
        {"type": "reversal", "path": file_path + file_key_regex + dispute_filename, "filename": dispute_filename},
        {"type": "fee", "path": file_path + file_key_regex + fee_filename, "filename": fee_filename},
    ]

    return return_data


def upload_transaction_data_to_s3_bucket(**kwargs):
    date_key = date.today()

    date_key_arr = str(date_key).split("-")
    year = date_key_arr[0]
    month = date_key_arr[1]
    dayy = date_key_arr[2]

    files_to_upload = kwargs['ti'].xcom_pull(task_ids='write_transaction_to_file')
    for file_to_upload in files_to_upload:
        if os.stat(file_to_upload['path']).st_size == 0:
            pass
        else:
            s3_folder = s3_folder_path.format(datasource_type=datasource_type, transaction_type=file_to_upload['type'],
                                              account_name=account_name,
                                              year=year,
                                              month=month, day=dayy)
            s3_path_to_upload = """s3://{s3_bucket}{s3_key}{fff}""".format(
                s3_bucket=s3_bucket, s3_key=s3_folder, fff=file_to_upload['filename'])

            upload_command = """AWS_ACCESS_KEY_ID={access_key} AWS_SECRET_ACCESS_KEY={secret_key} aws s3 mv {file_to_upload} {s3_path_to_upload}""".format(
                s3_path_to_upload=s3_path_to_upload, file_to_upload=file_to_upload['path'],
                access_key=aws_conn.extra_dejson['aws_access_key_id'],
                secret_key=aws_conn.extra_dejson['aws_secret_access_key'])
            os.system(upload_command)
            time.sleep(1)
    return True


t4 = PythonOperator(
    task_id='upload_transaction_data_to_s3_bucket',
    python_callable=upload_transaction_data_to_s3_bucket,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='write_transaction_to_file',
    python_callable=write_transaction_to_file,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='get_insight_from_klarna',
    python_callable=get_insight_from_klarna,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3 >> t4
