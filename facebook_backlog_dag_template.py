__author__ = 'Ayomide Aro'

from airflow import DAG
from airflow.models import Variable

from datetime import timedelta
from datetime import datetime, date
import time
import os

from airflow.operators.bash_operator import BashOperator
from airflow.operators.facebook_ads_plugin import FacebookAdsInsightsToS3Operator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': START_DATE,
    'email': ['admin@harwardmedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 23,
    'retry_delay': timedelta(minutes=20),
}

dag = DAG('DAG_NAME', default_args=default_args, schedule_interval=None)

dag_config = Variable.get('VARIABLES_NAME', deserialize_json=True)
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
date_preset = dag_config['date_preset']
account_id = dag_config['account_id']
access_token = dag_config['access_token']
api_version = dag_config['api_version']
insight_fields = dag_config['insight_fields']
action_attribution_windows = dag_config['action_attribution_windows']
file_path = dag_config['file_path']
time_increment = dag_config['time_increment']
backlogdays = dag_config['backlog_days']
days = 29 + int(dag_config['backlog_days'])
# today = datetime.today().strftime('%Y-%m-%d')
today = date.today().isoformat()
start_date = (date.today() - timedelta(days=29)).isoformat()
backlog_date = (date.today() - timedelta(days=days)).isoformat()

start_date_without_iso = (date.today() - timedelta(days=29))
backlog_date_without_iso = (date.today() - timedelta(days=days))
# file_key = 'act_{0}_backlog_{1}.json'.format(account_id, start_date)
file_key = 'act_{0}_backlog_DAY.json'.format(account_id)
s3_key = 'act_{}_backlog'.format(account_id)
s3_folder_path = "/{datasource_type}/{account_id}/{year}/{month}/{day}/"
file_key_regex = 'act_{0}_backlog/'.format(account_id)

print("Start Date ", start_date)
print("Backlog Date ", backlog_date)

t3 = FacebookAdsInsightsToS3Operator(
    task_id="get_insights_from_facebook",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    since=backlog_date,
    until=start_date,
    action_attribution_windows=action_attribution_windows,
    account_id=account_id,
    insight_fields=insight_fields,
    time_increment=time_increment,
    date_preset=date_preset,
    call_type="backlog",
    backlogdays=backlogdays,
    file_path=file_path + file_key_regex,
    file_key=file_key,
    access_token=access_token,
    api_version=api_version,
    dag=dag
)


def confirm_parameters(**kwargs):
    print(dag_config)
    return True


t2 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)


def create_files(**kwargs):
    delta = start_date_without_iso - backlog_date_without_iso

    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    for i in range(delta.days + 1):
        day = backlog_date_without_iso + timedelta(days=i)
        day_f = open(file_path + file_key_regex + str(file_key).replace("DAY", str(day)), 'w+')
        day_f.close()

    time.sleep(5)

    return True


t1 = PythonOperator(
    task_id='create_files',
    python_callable=create_files,
    dag=dag
)

t4_command = """find {path} -type f -size 0 | xargs rm -f""".format(path=file_path + file_key_regex)

t4 = BashOperator(
    task_id='check_empty_files',
    bash_command=t4_command,
    dag=dag
)


def upload_files_to_s3_bucket(**kwargs):
    files_to_upload = os.listdir(file_path + file_key_regex)

    for file_name in files_to_upload:
        file_name_arr = file_name.split("_")
        date_key = file_name_arr[3]
        date_key_arr = str(date_key).split("-")
        year = date_key_arr[0]
        month = date_key_arr[1]
        dayy = str(date_key_arr[2]).replace(".json", "")
        time.sleep(3)
        s3_folder = s3_folder_path.format(datasource_type=datasource_type, account_id=account_id+"_backlog", year=year,
                                          month=month, day=dayy)
        upload_command = """aws_access_key_id={access_key} aws_secret_access_key={secret_key} aws s3 mv {filepath} s3://{s3_bucket}{s3_key}data.json """.format(
            s3_bucket=s3_bucket, s3_key=s3_folder, filepath=file_path + file_key_regex + file_name,
            access_key=aws_conn.extra_dejson['aws_access_key_id'],
            secret_key=aws_conn.extra_dejson['aws_secret_access_key'])
        os.system(upload_command)
        time.sleep(2)

    return True


t5 = PythonOperator(
    task_id='upload_files_to_s3_bucket',
    python_callable=upload_files_to_s3_bucket,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5
