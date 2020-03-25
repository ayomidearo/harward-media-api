__author__ = 'Ayomide Aro'

import stripe
import redis
import os
import requests

from authorizenet import apicontractsv1
from authorizenet.apicontrollers import *
from decimal import *
from datetime import datetime, timedelta, date
import dateparser
import json

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
    'email': ['chris@harwardmedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'wait_for_downstream': True,
    'retry_delay': timedelta(minutes=3),
}

dag_name = "DAG_NAME"
dag = DAG(dag_name, catchup=False, default_args=default_args, schedule_interval="SCHEDULE_INTERVAL")

dag_config = Variable.get('VARIABLES_NAME', deserialize_json=True)
name = dag_config['api_login_id']
transaction_key = dag_config['transaction_key']
aws_conn = BaseHook.get_connection("aws_conn")
s3_bucket = dag_config['s3_bucket']
datasource_type = dag_config['datasource_type']
account_name = dag_config['account_name']
file_path = dag_config['file_path']
s3_folder_path = "/{datasource_type}/{transaction_type}/{account_name}/{year}/{month}/{day}/"

file_key_regex = 'authorize_transaction_{0}/'.format(account_name)

print("Starting job rn ")

timestamp = int(time.time() * 1000.0)

ENV = 'https://api2.authorize.net/xml/v1/request.api'

merchantAuth = apicontractsv1.merchantAuthenticationType()
merchantAuth.name = name
merchantAuth.transactionKey = transaction_key


def confirm_parameters(**kwargs):
    if not os.path.exists(file_path + file_key_regex):
        os.makedirs(file_path + file_key_regex)
    print(dag_config)
    return True


def back_log_days(**kwargs):
    backlogdays = str((date.today() - date(2018, 1, 1)).days)
    # backlogdays = "10"
    n_arr = []
    start_date = (date.today() - timedelta(days=0)).isoformat()

    q = int(backlogdays) // 30
    m = int(backlogdays) % 30

    if int(backlogdays) // 30 > 0:
        n_arr = [30] * q
        if m > 0:
            n_arr.append(m)
    else:
        n_arr.append(int(backlogdays))

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


def get_batch_ids(ds, **kwargs):
    rd = redis.Redis(host='redis', port=6379, db=1)

    if rd.exists(dag_name) == 1:
        batch_ids = []

        tttt = rd.hget(dag_name, "last_date")
        ddd = dateparser.parse(tttt.decode("utf-8"))
        eee = datetime.now()

        print("Starting Date >>>>>> ", ddd)
        print("Ending Date >>>>>> ", eee)
        settledBatchListRequest = apicontractsv1.getSettledBatchListRequest()
        settledBatchListRequest.merchantAuthentication = merchantAuth
        # settledBatchListRequest.firstSettlementDate = datetime.now() - timedelta(days=31)
        # settledBatchListRequest.firstSettlementDate = dateparser.parse('2020-02-10')
        settledBatchListRequest.firstSettlementDate = ddd
        settledBatchListRequest.lastSettlementDate = eee
        # settledBatchListRequest.lastSettlementDate = datetime.now()

        settledBatchListController = getSettledBatchListController(settledBatchListRequest)
        settledBatchListController.setenvironment(ENV)

        settledBatchListController.execute()

        settledBatchListResponse = settledBatchListController.getresponse()

        if settledBatchListResponse is not None:
            if settledBatchListResponse.messages.resultCode == apicontractsv1.messageTypeEnum.Ok:
                print('Successfully got settled batch list!')
                try:

                    for batchItem in settledBatchListResponse.batchList.batch:
                        batch_ids.append(batchItem.batchId)

                    if settledBatchListResponse.messages is not None:
                        print('Message Code : %s' % settledBatchListResponse.messages.message[0].code)
                        print('Message Text : %s' % settledBatchListResponse.messages.message[0].text)

                except:
                    print('No records >>>>>>  count >>>>>>> ')
            else:
                if settledBatchListResponse.messages is not None:
                    print('Failed to get settled batch list.\nCode:%s \nText:%s' % (
                        settledBatchListResponse.messages.message[0].code,
                        settledBatchListResponse.messages.message[0].text))
        rd.hset(dag_name, "last_date", str(eee))
        print("Last saved date for rerun >>>>>>>>>>>> ", str(rd.hget(dag_name, "last_date")))

        return batch_ids
    else:
        batch_ids = []
        back = kwargs['ti'].xcom_pull(task_ids='back_log_days')

        for d in back:
            ddd = dateparser.parse(d['start'])
            eee = dateparser.parse(d['end'])
            settledBatchListRequest = apicontractsv1.getSettledBatchListRequest()
            settledBatchListRequest.merchantAuthentication = merchantAuth
            # settledBatchListRequest.firstSettlementDate = datetime.now() - timedelta(days=31)
            # settledBatchListRequest.firstSettlementDate = dateparser.parse('2020-02-10')
            settledBatchListRequest.firstSettlementDate = ddd
            settledBatchListRequest.lastSettlementDate = eee
            # settledBatchListRequest.lastSettlementDate = datetime.now()

            settledBatchListController = getSettledBatchListController(settledBatchListRequest)
            settledBatchListController.setenvironment(ENV)

            settledBatchListController.execute()

            settledBatchListResponse = settledBatchListController.getresponse()

            if settledBatchListResponse is not None:
                if settledBatchListResponse.messages.resultCode == apicontractsv1.messageTypeEnum.Ok:
                    print('Successfully got settled batch list!')
                    try:

                        for batchItem in settledBatchListResponse.batchList.batch:
                            batch_ids.append(batchItem.batchId)

                        if settledBatchListResponse.messages is not None:
                            print('Message Code : %s' % settledBatchListResponse.messages.message[0].code)
                            print('Message Text : %s' % settledBatchListResponse.messages.message[0].text)

                    except:
                        print('No records >>>>>>  count >>>>>>> ')
                else:
                    if settledBatchListResponse.messages is not None:
                        print('Failed to get settled batch list.\nCode:%s \nText:%s' % (
                            settledBatchListResponse.messages.message[0].code,
                            settledBatchListResponse.messages.message[0].text))
            rd.hset(dag_name, "last_date", str(eee))
            print("Last saved date >>>>>>>>>>>> ", str(rd.hget(dag_name, "last_date")))

        return batch_ids


def get_transaction_list(ds, **kwargs):
    batch_ids = kwargs['ti'].xcom_pull(task_ids='get_batch_ids')

    transaction_ids = []
    for b in batch_ids:
        transactionListRequest = apicontractsv1.getTransactionListRequest()
        transactionListRequest.merchantAuthentication = merchantAuth
        transactionListRequest.batchId = str(b)
        paging = apicontractsv1.Paging()
        paging.limit = '1000'
        paging.offset = '1'
        transactionListRequest.paging = paging

        transactionListController = getTransactionListController(transactionListRequest)
        transactionListController.setenvironment(ENV)

        transactionListController.execute()

        transactionListResponse = transactionListController.getresponse()

        if transactionListResponse is not None:
            if transactionListResponse.messages.resultCode == apicontractsv1.messageTypeEnum.Ok:
                if hasattr(transactionListResponse, 'transactions'):
                    print('Successfully got transaction list!')
                    try:

                        for transaction in transactionListResponse.transactions.transaction:
                            transaction_ids.append(transaction.transId)

                    except:
                        print('No records in batch id')
                    for transaction in transactionListResponse.transactions.transaction:
                        transaction_ids.append(transaction.transId)

                if transactionListResponse.messages is not None:
                    print('Message Code : %s' % transactionListResponse.messages.message[0].code)
                    print('Message Text : %s' % transactionListResponse.messages.message[0].text)
            else:
                if transactionListResponse.messages is not None:
                    print('Failed to get transaction list.\nCode:%s \nText:%s' % (
                        transactionListResponse.messages.message[0].code,
                        transactionListResponse.messages.message[0].text))

    return transaction_ids


def get_transaction_details(ds, **kwargs):
    t_list = kwargs['ti'].xcom_pull(task_ids='get_transaction_list')

    transaction_details_list = []
    print("Length of transactions >>>>>> ", len(t_list))
    for t in t_list:
        transactionDetailsRequest = apicontractsv1.getTransactionDetailsRequest()
        transactionDetailsRequest.merchantAuthentication = merchantAuth
        transactionDetailsRequest.transId = str(t)

        transactionDetailsController = getTransactionDetailsController(transactionDetailsRequest)
        transactionDetailsController.setenvironment(ENV)

        transactionDetailsController.execute()

        transactionDetailsResponse = transactionDetailsController.getresponse()

        if transactionDetailsResponse is not None:
            if transactionDetailsResponse.messages.resultCode == apicontractsv1.messageTypeEnum.Ok:
                print('Successfully got transaction details!')

                t = {
                    "transaction_id": str(transactionDetailsResponse.transaction.transId),
                    "transaction_type": str(transactionDetailsResponse.transaction.transactionType),
                    "transaction_status": str(transactionDetailsResponse.transaction.transactionStatus),
                    "auth_amount": float(transactionDetailsResponse.transaction.authAmount),
                    "settlement_amount": float(transactionDetailsResponse.transaction.settleAmount),
                    "date": str(transactionDetailsResponse.transaction.submitTimeUTC)
                }

                if hasattr(transactionDetailsResponse.transaction, 'tax'):
                    t['tax'] = float(transactionDetailsResponse.transaction.tax.amount)
                if hasattr(transactionDetailsResponse.transaction, 'profile'):
                    t['customer_profile_id'] = str(transactionDetailsResponse.transaction.profile.customerProfileId)

                transaction_details_list.append(t)

                if transactionDetailsResponse.messages is not None:
                    print('Message Code : %s' % transactionDetailsResponse.messages.message[0].code)
                    print('Message Text : %s' % transactionDetailsResponse.messages.message[0].text)
            else:
                if transactionDetailsResponse.messages is not None:
                    print('Failed to get transaction details.\nCode:%s \nText:%s' % (
                        transactionDetailsResponse.messages.message[0].code,
                        transactionDetailsResponse.messages.message[0].text))

    return transaction_details_list


def write_transaction_to_file(**kwargs):
    transaction_filename = 'transaction_data_{0}.json'.format(timestamp)
    transaction_f = open(file_path + file_key_regex + transaction_filename, 'w+')

    trxs = kwargs['ti'].xcom_pull(task_ids='get_transaction_details')
    for trx in trxs:
        transaction_f.write(json.dumps(trx) + "\n")

    transaction_f.close()

    return_data = [
        {"type": "transaction", "path": file_path + file_key_regex + transaction_filename,
         "filename": transaction_filename}
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


t7 = PythonOperator(
    task_id='upload_transaction_data_to_s3_bucket',
    python_callable=upload_transaction_data_to_s3_bucket,
    provide_context=True,
    dag=dag
)

t6 = PythonOperator(
    task_id='write_transaction_to_file',
    python_callable=write_transaction_to_file,
    provide_context=True,
    dag=dag
)

t5 = PythonOperator(
    task_id='get_transaction_details',
    python_callable=get_transaction_details,
    provide_context=True,
    dag=dag)

t4 = PythonOperator(
    task_id='get_transaction_list',
    python_callable=get_transaction_list,
    provide_context=True,
    dag=dag)

t3 = PythonOperator(
    task_id='get_batch_ids',
    python_callable=get_batch_ids,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='back_log_days',
    python_callable=back_log_days,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
