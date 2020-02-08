__author__ = 'Ayomide Aro'

import os
import json
import boto3

from airflow import DAG
from airflow.models import Variable

from datetime import timedelta
from datetime import datetime, date
import time

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
    'retry_delay': timedelta(minutes=60),
}

dag = DAG('DAG_NAME', catchup=False, default_args=default_args, schedule_interval="SCHEDULE_INTERVAL")
# dag = DAG('act_1259601010838576_daily_run_dag', default_args=default_args, schedule_interval=None)

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
s3_folder_path = "/{datasource_type}/{account_id}/{year}/{month}/{day}/"

beginning_of_year = date(date.today().year, 1, 1)
end_of_year = date(date.today().year, 12, 31)

client = boto3.client(
    'lambda',
    aws_access_key_id=aws_conn.extra_dejson['aws_access_key_id'],
    aws_secret_access_key=aws_conn.extra_dejson['aws_secret_access_key'],
    region_name='us-west-2'
)

dates_and_preset = {
    "today": "0",
    "yesterday": "1",
    "this_month": str((date.today() - date(date.today().year, date.today().month, 1)).days),
    "last_month": str((date.today() - date(date.today().year, date.today().month - 1, 1)).days),
    "this_quarter": "90",
    "lifetime": "900",
    "last_3d": "3",
    "last_7d": "7",
    "last_14d": "14",
    "last_28d": "28",
    "last_30d": "30",
    "last_90d": "90",
    "last_week_mon_sun": "7",
    "last_quarter": "92",
    "last_year": str((date.today() - date(date.today().year-1, 1, 1)).days),
    "this_week_mon_today": "7",
    "this_week_sun_today": "7",
    "this_year": str((date.today() - beginning_of_year).days),
}

# today = datetime.today().strftime('%Y-%m-%d')
today = date.today().isoformat()
today_without_iso = date.today()


the_day = int(dates_and_preset[date_preset])

since = (date.today() - timedelta(days=the_day)).isoformat()
since_without_iso = (date.today() - timedelta(days=the_day))


file_key = 'act_{0}_DAY.json'.format(account_id)
file_key_regex = 'act_{0}/'.format(account_id)
s3_key = 'act_{}'.format(account_id)

print("Today is >>> ", today)
print("Since is >>> ", since)

print("Starting job rn ")

t3 = FacebookAdsInsightsToS3Operator(
    task_id="get_insights_from_facebook",
    s3_bucket=s3_bucket,
    s3_key=s3_key,
    since=since,
    until=today,
    action_attribution_windows=action_attribution_windows,
    account_id=account_id,
    insight_fields=insight_fields,
    time_increment=time_increment,
    date_preset=date_preset,
    call_type="daily",
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
    delta = today_without_iso - since_without_iso

    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)

    for i in range(delta.days + 1):
        day = since_without_iso + timedelta(days=i)
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

    payload = json.dumps({"account_id": account_id, "file_names": files_to_upload})
    response_from_lambda = client.invoke(
        FunctionName='deleteOldRecordsAndTriggerGlue',
        InvocationType='Event',
        LogType='Tail',
        Payload=payload
    )

    print("Response from lambda ", response_from_lambda)

    for file_name in files_to_upload:
        file_name_arr = file_name.split("_")
        date_key = file_name_arr[2]
        date_key_arr = str(date_key).split("-")
        year = date_key_arr[0]
        month = date_key_arr[1]
        dayy = str(date_key_arr[2]).replace(".json", "")
        time.sleep(3)
        s3_folder = s3_folder_path.format(datasource_type=datasource_type, account_id=account_id,
                                          year=year,
                                          month=month, day=dayy)
        upload_command = """aws_access_key_id={access_key} aws_secret_access_key={secret_key} aws s3 mv {filepath} s3://{s3_bucket}{s3_key}data.json""".format(
            s3_bucket=s3_bucket, s3_key=s3_folder, filepath=file_path + file_key_regex + file_name,
            access_key=aws_conn.extra_dejson['aws_access_key_id'],
            secret_key=aws_conn.extra_dejson['aws_secret_access_key'])
        os.system(upload_command)
        time.sleep(1)

    return True


t5 = PythonOperator(
    task_id='upload_files_to_s3_bucket',
    python_callable=upload_files_to_s3_bucket,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5
