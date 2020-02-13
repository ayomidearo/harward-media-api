__author__ = 'Ayomide Aro'

import json
import stripe
import redis
import os

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
# dag = DAG('act_1259601010838576_daily_run_dag', default_args=default_args, schedule_interval=None)

dag_config = Variable.get('VARIABLES_NAME', deserialize_json=True)
stripe.api_key = dag_config['stripe_key']
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
account_name = dag_config['account_name']
file_path = dag_config['file_path']
s3_folder_path = "/{datasource_type}/{transaction_type}/{account_name}/{year}/{month}/{day}/"

# file_key = 'act_{0}_DAY.json'.format(account_name)
file_key_regex = 'stripe_charge_{0}/'.format(account_name)
# s3_key = 'act_{}'.format(account_name)

print("Starting job rn ")

timestamp = int(time.time() * 1000.0)

date_key = date.today()

date_key_arr = str(date_key).split("-")
year = date_key_arr[0]
month = date_key_arr[1]
dayy = date_key_arr[2]

s3_folder = s3_folder_path.format(datasource_type=datasource_type, transaction_type='charge', account_name=account_name,
                                  year=year,
                                  month=month, day=dayy)


def get_insight_from_stripe(ds, **kwargs):
    r = redis.Redis(host='redis', port=6379, db=1)

    if r.exists(dag_name) == 1:
        filename = 'charge_data_{0}.json'.format(timestamp)
        f = open(file_path + file_key_regex + filename, 'w+')
        new_data = stripe.Charge.list(ending_before=r.hget(dag_name, "last_obj_id"), expand=['data.balance_transaction'])
        if len(new_data['data']) == 0:
            f.close()
            return filename
        else:
            last_obj_id = new_data['data'][0]['id']

            print(last_obj_id)

            for new_data_item in new_data.auto_paging_iter():
                f.write(json.dumps(new_data_item) + "\n")

            f.close()

            r.hset(dag_name, "last_obj_id", last_obj_id)
            return filename
    else:
        filename = 'charge_data_{0}.json'.format(timestamp)
        f = open(file_path + file_key_regex + filename, 'w+')
        backlog_data = stripe.Charge.list(expand=['data.balance_transaction'])
        if len(backlog_data['data']) == 0:
            f.close()
            return filename
        else:
            last_obj_id = backlog_data['data'][0]['id']

            for back_log_d in backlog_data.auto_paging_iter():
                f.write(json.dumps(back_log_d) + "\n")

            f.close()
            r.hset(dag_name, "last_obj_id", last_obj_id)
            return filename



def upload_charge_data_to_s3_bucket(**kwargs):
    date_key = date.today()

    date_key_arr = str(date_key).split("-")
    year = date_key_arr[0]
    month = date_key_arr[1]
    dayy = date_key_arr[2]
    s3_folder = s3_folder_path.format(datasource_type=datasource_type, transaction_type='charge',
                                      account_name=account_name,
                                      year=year,
                                      month=month, day=dayy)

    fff = kwargs['ti'].xcom_pull(task_ids='get_insight_from_stripe')
    file_to_upload = "{filepath}".format(filepath=file_path + file_key_regex) + fff

    s3_path_to_upload = """s3://{s3_bucket}{s3_key}{fff}""".format(
        s3_bucket=s3_bucket, s3_key=s3_folder, fff=fff)

    if os.stat(file_to_upload).st_size == 0:
        return True
    else:
        upload_command = """AWS_ACCESS_KEY_ID={access_key} AWS_SECRET_ACCESS_KEY={secret_key} aws s3 mv {file_to_upload} {s3_path_to_upload}""".format(
            s3_path_to_upload=s3_path_to_upload, file_to_upload=file_to_upload,
            access_key=aws_conn.extra_dejson['aws_access_key_id'],
            secret_key=aws_conn.extra_dejson['aws_secret_access_key'])
        os.system(upload_command)
        time.sleep(1)
        return True


t3 = PythonOperator(
    task_id='upload_charge_data_to_s3_bucket',
    python_callable=upload_charge_data_to_s3_bucket,
    provide_context=True,
    dag=dag
)

t2 = PythonOperator(
    task_id='get_insight_from_stripe',
    python_callable=get_insight_from_stripe,
    provide_context=True,
    dag=dag)


def confirm_parameters(**kwargs):
    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    print(dag_config)
    return True


t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3
