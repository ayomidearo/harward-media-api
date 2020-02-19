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
import base64
import dateparser

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
client_id = dag_config['client_id']
client_secret = dag_config['client_secret']
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
account_name = dag_config['account_name']
file_path = dag_config['file_path']
s3_folder_path = "/{datasource_type}/{transaction_type}/{account_name}/{year}/{month}/{day}/"

file_key_regex = 'paypal_transaction_{0}/'.format(account_name)

print("Starting job rn ")

timestamp = int(time.time() * 1000.0)

BASE_URL = "https://api.paypal.com"

transaction_url = BASE_URL + "/v1/reporting/transactions"
token_url = BASE_URL + "/v1/oauth2/token"


def confirm_parameters(**kwargs):
    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    print(dag_config)
    return True


def get_access_token(**kwargs):
    credentials = "%s:%s" % (client_id, client_secret)
    encode_credential = base64.b64encode(credentials.encode('utf-8')).decode('utf-8').replace("\n", "")

    headers = {
        "Authorization": ("Basic %s" % encode_credential),
        'Accept': 'application/json',
        'Accept-Language': 'en_US',
    }

    param = {
        'grant_type': 'client_credentials',
    }

    url = 'https://api.paypal.com/v1/oauth2/token'

    r = requests.post(url, headers=headers, data=param)

    body = r.json()

    access_token = body['access_token']

    return access_token


def back_log_days(**kwargs):
    backlogdays = "1095"
    n_arr = []
    start_date = (date.today() - timedelta(days=0)).isoformat()

    q = int(backlogdays) // 30
    m = int(backlogdays) % 30

    if int(backlogdays) // 30 > 0:
        n_arr = [30] * q
        if m > 0:
            n_arr.append(m)

    print(n_arr)

    days_to_query = [start_date]
    k = 0
    for d in n_arr:
        ddd = k + d
        days_to_query.append((date.today() - timedelta(days=ddd)).isoformat())
        k = ddd

    new_arr = sorted(days_to_query)
    query = []
    while len(new_arr) >= 2:
        start = new_arr[0]
        end = new_arr[1]
        query.append({"start": start, "end": end})
        new_arr.pop(0)

    return query


def get_insight_from_paypal(ds, **kwargs):
    rd = redis.Redis(host='redis', port=6379, db=2)

    access_token = kwargs['ti'].xcom_pull(task_ids='get_access_token')

    if rd.exists(dag_name) == 1:
        ttt = []

        eee = dateparser.parse((date.today() - timedelta(days=0)).isoformat() + " 11:59pm")

        params = {'start_date': rd.hget(dag_name, "last_date"), 'end_date': str(eee.isoformat("T")) + "Z",
                  'fields': 'all'}

        print(params)

        headers = {
            "Authorization": "Bearer {token}".format(token=access_token),
            'Accept': 'application/json'
        }

        url = 'https://api.paypal.com/v1/reporting/transactions'

        r = requests.get(url, headers=headers, params=params)

        print(r.text)

        body = r.json()
        count = 1
        page = 2
        while count < body['total_pages']:
            ttt.extend(body['transaction_details'])
            params = {'start_date': rd.hget(dag_name, "last_date"), 'end_date': str(eee.isoformat("T")) + "Z",
                      'fields': 'all', 'page_size': 100, 'page': page}

            print(params)

            headers = {
                "Authorization": "Bearer {token}".format(token=access_token),
                'Accept': 'application/json'
            }
            r = requests.get(url, headers=headers, params=params)

            r.raise_for_status()
            body = r.json()
            count = count + 1
            page = page + 1

        ttt.extend(body['transaction_details'])

        return ttt
    else:
        ttt = []
        back = kwargs['ti'].xcom_pull(task_ids='back_log_days')

        for d in back:
            ddd = dateparser.parse(d['start'])
            eee = dateparser.parse(d['end'])
            print(ddd)
            print(eee)

            params = {'start_date': str(ddd.isoformat("T")) + "Z", 'end_date': str(eee.isoformat("T")) + "Z",
                      'fields': 'all'}

            print(params)

            headers = {
                "Authorization": "Bearer {token}".format(token=access_token),
                'Accept': 'application/json'
            }

            url = 'https://api.paypal.com/v1/reporting/transactions'

            r = requests.get(url, headers=headers, params=params)

            body = r.json()
            count = 1
            page = 2
            while count < body['total_pages']:
                ttt.extend(body['transaction_details'])
                params = {'start_date': str(ddd.isoformat("T")) + "Z", 'end_date': str(eee.isoformat("T")) + "Z",
                          'fields': 'all', 'page_size': 100, 'page': page}

                print(params)

                headers = {
                    "Authorization": "Bearer {token}".format(token=access_token),
                    'Accept': 'application/json'
                }
                r = requests.get(url, headers=headers, params=params)

                r.raise_for_status()
                body = r.json()
                count = count + 1
                page = page + 1

            ttt.extend(body['transaction_details'])
            rd.hset(dag_name, "last_date", body["last_refreshed_datetime"])

        return ttt


def write_transaction_to_file(**kwargs):
    transaction_filename = 'transaction_data_{0}.json'.format(timestamp)
    transaction_f = open(file_path + file_key_regex + transaction_filename, 'w+')

    trxs = kwargs['ti'].xcom_pull(task_ids='get_insight_from_paypal')
    for trx in trxs:
        transaction_f.write(json.dumps(trx) + "\n")

    transaction_f.close()

    return_data = [
        {"type": "transaction", "path": file_path + file_key_regex + transaction_filename, "filename": transaction_filename}
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


t6 = PythonOperator(
    task_id='upload_transaction_data_to_s3_bucket',
    python_callable=upload_transaction_data_to_s3_bucket,
    provide_context=True,
    dag=dag
)

t5 = PythonOperator(
    task_id='write_transaction_to_file',
    python_callable=write_transaction_to_file,
    provide_context=True,
    dag=dag
)

t4 = PythonOperator(
    task_id='get_insight_from_paypal',
    python_callable=get_insight_from_paypal,
    provide_context=True,
    dag=dag)

t3 = PythonOperator(
    task_id='back_log_days',
    python_callable=back_log_days,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='get_access_token',
    python_callable=get_access_token,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
