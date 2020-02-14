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
shopify_api_key = dag_config['shopify_api_key']
shopify_password = dag_config['shopify_password']
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
shop_name = dag_config['shop_name']
file_path = dag_config['file_path']
s3_folder_path = "/{datasource_type}/{transaction_type}/{shop_name}/{year}/{month}/{day}/"

file_key_regex = 'shopify_transaction_{0}/'.format(shop_name)

print("Starting job rn ")

timestamp = int(time.time() * 1000.0)

shop_url = "https://{API_KEY}:{PASSWORD}@{SHOP_NAME}.myshopify.com/admin".format(API_KEY=shopify_api_key,
                                                                                 PASSWORD=shopify_password,
                                                                                 SHOP_NAME=shop_name)

transaction_url = shop_url + "/api/2020-01/shopify_payments/balance/transactions.json"


def confirm_parameters(**kwargs):
    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    print(dag_config)
    return True


def get_insight_from_shopify(ds, **kwargs):
    r = redis.Redis(host='redis', port=6379, db=1)

    if r.exists(dag_name) == 1:
        transactions = []
        res = requests.get(
            transaction_url + "?limit=250&since_id={since_id}".format(since_id=r.hget(dag_name, "since_id")))
        insight_response_body = res.json()

        if res.headers['Link']:
            cursor = str(res.headers['Link']).split(',')

            next_urls = [_ for _ in cursor if _.find('next') > 0]

            count = 0

            while len(next_urls) > 0:
                time.sleep(2)
                transactions.extend(insight_response_body['transactions'])
                print("Next url count {count} ".format(count=count), cursor)
                next_url = next_urls[0].split(';')[0].strip('<>')
                params = str(next_url).split("?")[1]
                res = requests.get(transaction_url + "?" + params)
                print("Count {count} ".format(count=count), res.headers['X-Shopify-Shop-Api-Call-Limit'])
                print("Params count {count} ".format(count=count), params)
                res.raise_for_status()
                insight_response_body = res.json()
                cursor = str(res.headers['Link']).split(',')
                next_urls = [_ for _ in cursor if _.find('next') > 0]
                count = count + 1

            transactions.extend(insight_response_body['transactions'])

            r.hset(dag_name, "since_id", transactions[-1]['id'])

            return transactions
        else:
            return transactions
    else:
        transactions = []
        res = requests.get(transaction_url + "?limit=250")
        insight_response_body = res.json()

        cursor = str(res.headers['Link']).split(',')

        next_urls = [_ for _ in cursor if _.find('next') > 0]

        count = 0

        while len(next_urls) > 0:
            time.sleep(2)
            transactions.extend(insight_response_body['transactions'])
            print("Next url count {count} ".format(count=count), cursor)
            next_url = next_urls[0].split(';')[0].strip('<>')
            params = str(next_url).split("?")[1]
            res = requests.get(transaction_url + "?" + params)
            print("Count {count} ".format(count=count), res.headers['X-Shopify-Shop-Api-Call-Limit'])
            print("Params count {count} ".format(count=count), params)
            res.raise_for_status()
            insight_response_body = res.json()
            cursor = str(res.headers['Link']).split(',')
            next_urls = [_ for _ in cursor if _.find('next') > 0]
            count = count + 1

        transactions.extend(insight_response_body['transactions'])

        r.hset(dag_name, "since_id", transactions[0]['id'])

        return transactions


def write_transaction_to_file(**kwargs):
    charge_filename = 'charge_data_{0}.json'.format(timestamp)
    refund_filename = 'refund_data_{0}.json'.format(timestamp)
    dispute_filename = 'dispute_data_{0}.json'.format(timestamp)
    charge_f = open(file_path + file_key_regex + charge_filename, 'w+')
    refund_f = open(file_path + file_key_regex + refund_filename, 'w+')
    dispute_f = open(file_path + file_key_regex + dispute_filename, 'w+')

    trxs = kwargs['ti'].xcom_pull(task_ids='get_insight_from_shopify')
    for trx in trxs:
        if trx['type'] == "charge":
            charge_f.write(json.dumps(trx) + "\n")
        elif trx['type'] == "refund":
            refund_f.write(json.dumps(trx) + "\n")
        elif trx['type'] == "dispute":
            dispute_f.write(json.dumps(trx) + "\n")

    charge_f.close()
    refund_f.close()
    dispute_f.close()

    return_data = [
        {"type": "charge", "path": file_path + file_key_regex + charge_filename, "filename": charge_filename},
        {"type": "refund", "path": file_path + file_key_regex + refund_filename, "filename": refund_filename},
        {"type": "dispute", "path": file_path + file_key_regex + dispute_filename, "filename": dispute_filename},
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
                                              shop_name=shop_name,
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
    task_id='get_insight_from_shopify',
    python_callable=get_insight_from_shopify,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3 >> t4
