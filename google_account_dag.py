__author__ = 'Ayomide Aro'

import os
import json
import boto3
import redis
import pandas as pd
import io
from googleads import adwords

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
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
date_preset = dag_config['date_preset']
account_id = dag_config['account_id']
account_name = dag_config['account_name']
file_path = dag_config['file_path']
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

# today = datetime.today().strftime('%Y-%m-%d')
today = date.today().isoformat()
today_without_iso = date.today()

the_day = int(date_preset)

since = (date.today() - timedelta(days=the_day)).isoformat()
back = (date.today() - timedelta(days=int(backlogdays))).isoformat()
# since_without_iso = (date.today() - timedelta(days=the_day))


file_key = 'google_{0}_DAY.json'.format(account_id)
file_key_regex = 'google_{0}/'.format(account_id)
s3_key = 'google_{}'.format(account_id)

print("Today is >>> ", today)
print("Since is >>> ", since)
print("Backlog is >>> ", since)

print("Starting job rn ")

rd = redis.Redis(host='redis', port=6379, db=5)


def confirm_parameters(**kwargs):
    print(dag_config)
    return True


def create_files(**kwargs):
    if rd.exists(dag_name) == 1:
        since_without_iso = (date.today() - timedelta(days=the_day))
        delta = today_without_iso - since_without_iso
    else:
        since_without_iso = (date.today() - timedelta(days=int(backlogdays)))
        delta = today_without_iso - since_without_iso

    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)

    for i in range(delta.days + 1):
        day = since_without_iso + timedelta(days=i)
        day_f = open(file_path + file_key_regex + str(file_key).replace("DAY", str(day)), 'w+')
        day_f.close()

    time.sleep(1)

    return True


def get_report_from_google(ds, **kwargs):
    if rd.exists(dag_name) == 1:
        start = datetime.today().date().isoformat().replace("-", "")
        end = datetime.now() + timedelta(days=- the_day)
        end = end.date().isoformat().replace("-", "")
        ddd = end + ',' + start
        # Define output as a string
        output = io.StringIO()

        # Initialize client object.
        adwords_client = adwords.AdWordsClient.LoadFromStorage('/usr/local/airflow/keys/YAML_FILE')

        adwords_client.SetClientCustomerId(account_id)

        report_downloader = adwords_client.GetReportDownloader(version='v201809')

        report_query = (adwords.ReportQueryBuilder()
                        .Select('Id', 'AdGroupId', 'CampaignId', 'CampaignName', 'Date', 'DayOfWeek', 'Clicks',
                                'Impressions', 'Cost', 'Conversions', 'ConversionValue')
                        .From('AD_PERFORMANCE_REPORT')
                        .Where('CampaignStatus').In('ENABLED')
                        .During(ddd)
                        .Build())

        print(report_query)

        report_downloader.DownloadReportWithAwql(report_query, 'CSV', output, skip_report_header=True,
                                                 skip_column_header=False, skip_report_summary=True,
                                                 include_zero_impressions=False)

        output.seek(0)

        types = {'CampaignId': pd.np.int64, 'Clicks': pd.np.int32, 'Impressions': pd.np.int32,
                 'Cost': pd.np.float64, 'Conversions': pd.np.float64, 'ConversionValue': pd.np.float64}

        df = pd.read_csv(output, low_memory=False, dtype=types, na_values=[' --'])

        rd.hset(dag_name, "account_id", account_id)
        return df
    else:
        start = datetime.today().date().isoformat().replace("-", "")
        end = datetime.now() + timedelta(days=- int(backlogdays))
        end = end.date().isoformat().replace("-", "")
        ddd = end + ',' + start
        # Define output as a string
        output = io.StringIO()

        # Initialize client object.
        adwords_client = adwords.AdWordsClient.LoadFromStorage('/usr/local/airflow/keys/googleads.yaml')

        adwords_client.SetClientCustomerId(account_id)

        report_downloader = adwords_client.GetReportDownloader(version='v201809')

        report_query = (adwords.ReportQueryBuilder()
                        .Select('Id', 'AdGroupId', 'CampaignId', 'CampaignName', 'Date', 'DayOfWeek', 'Clicks',
                                'Impressions', 'Cost', 'Conversions', 'ConversionValue')
                        .From('AD_PERFORMANCE_REPORT')
                        .Where('CampaignStatus').In('ENABLED')
                        .During(ddd)
                        .Build())

        print(report_query)

        report_downloader.DownloadReportWithAwql(report_query, 'CSV', output, skip_report_header=True,
                                                 skip_column_header=False, skip_report_summary=True,
                                                 include_zero_impressions=False)

        output.seek(0)

        types = {'CampaignId': pd.np.int64, 'Clicks': pd.np.int32, 'Impressions': pd.np.int32,
                 'Cost': pd.np.float64, 'Conversions': pd.np.float64, 'ConversionValue': pd.np.float64}

        df = pd.read_csv(output, low_memory=False, dtype=types, na_values=[' --'])

        rd.hset(dag_name, "account_id", account_id)
        return df


def write_report_to_file(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='get_report_from_google')
    for row in df.iterrows():
        date_start = row[1]['Day']
        file_name = file_path + file_key_regex + str(file_key).replace("DAY", date_start)
        with open(file_name, 'a+') as report_file:
            row[1].to_json(report_file)
            report_file.write("\n")

    return True


t5_command = """find {path} -type f -size 0 | xargs rm -f""".format(path=file_path + file_key_regex)


def upload_files_to_s3_bucket(**kwargs):
    files_to_upload = os.listdir(file_path + file_key_regex)

    payload = json.dumps({"account_id": account_id, "file_names": files_to_upload})
    response_from_lambda = client.invoke(
        FunctionName='deleteOldRecordsFromGoogleAndTriggerGlue',
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
        s3_folder = s3_folder_path.format(datasource_type=datasource_type, account_id=account_id,
                                          year=year,
                                          month=month, day=dayy)
        upload_command = """AWS_ACCESS_KEY_ID={access_key} AWS_SECRET_ACCESS_KEY={secret_key} aws s3 mv {filepath} s3://{s3_bucket}{s3_key}data.json""".format(
            s3_bucket=s3_bucket, s3_key=s3_folder, filepath=file_path + file_key_regex + file_name,
            access_key=aws_conn.extra_dejson['aws_access_key_id'],
            secret_key=aws_conn.extra_dejson['aws_secret_access_key'])
        os.system(upload_command)
        time.sleep(1)

    return True


t6 = PythonOperator(
    task_id='upload_files_to_s3_bucket',
    python_callable=upload_files_to_s3_bucket,
    dag=dag
)

t5 = BashOperator(
    task_id='check_empty_files',
    bash_command=t5_command,
    dag=dag
)

t4 = PythonOperator(
    task_id='write_report_to_file',
    python_callable=write_report_to_file,
    provide_context=True,
    dag=dag
)

t3 = PythonOperator(
    task_id='get_report_from_google',
    python_callable=get_report_from_google,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='create_files',
    python_callable=create_files,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
