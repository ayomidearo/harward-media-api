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

from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
import time
import bs4

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
dag = DAG(dag_name, catchup=False, default_args=default_args, schedule_interval="SCHEDULE_INTERVAL", max_active_runs=1)

dag_config = Variable.get('VARIABLES_NAME', deserialize_json=True)
email = dag_config['user_email']
password = dag_config['user_password']
account_name = dag_config['account_name']

postgres_hook = PostgresHook("harward")


def confirm_parameters(**kwargs):
    print(dag_config)
    return True


def check_driver(**kwargs):
    if os.path.isfile('/usr/local/airflow/keys/geckodriver'):
        print("Driver is available")
    else:
        print("Can't find Driver")

    return True


def get_report_from_funnel(ds, **kwargs):
    connection = postgres_hook.get_conn()
    options = Options()
    options.headless = True
    browser = webdriver.Firefox(options=options, executable_path='/usr/local/airflow/keys/geckodriver')

    cursor = connection.cursor()

    browser.get('https://app.clickfunnels.com/users/sign_in')
    user_email = browser.find_element_by_id("user_email")
    user_email.send_keys(email)
    user_password = browser.find_element_by_id("user_password")
    user_password.send_keys(password)
    user_password.send_keys(Keys.ENTER)
    delay = 20

    try:
        browser.implicitly_wait(20)
        WebDriverWait(browser, delay).until(EC.presence_of_element_located((By.CLASS_NAME, 'quick-links')))

    except TimeoutException:
        print("Loading took too much time!")

    ClickFunnelsLink = browser.find_element_by_link_text('Click Funnels')
    ClickFunnelsLink.click()
    htmlContents = browser.find_elements_by_class_name("groupForFunnel")
    for htmlContent in htmlContents:

        groupName = htmlContent.find_element_by_class_name('title').text
        allFunnels = htmlContent.find_elements_by_class_name('pageListingItem')

        for allFunnel in allFunnels:
            funnelName = allFunnel.find_element_by_class_name('pageListingTitle').text
            lastUpdated = allFunnel.find_element_by_class_name('timeago').get_attribute('title')
            steps = allFunnel.find_element_by_class_name('funnelStatSmall').text
            stepLink = allFunnel.find_element_by_class_name('btn-edit')
            stepLinkUrl = stepLink.get_attribute('href')
            urlData = stepLinkUrl.split('/')
            funnelId = str(urlData[-1])
            browser.execute_script("window.open('" + stepLinkUrl + "');")
            time.sleep(10)
            browser.switch_to.window(browser.window_handles[1])
            time.sleep(10)
            sideSortList = browser.find_element_by_class_name('sideSortList')
            visitUrl = sideSortList.find_element_by_xpath("//a[@data-original-title='Visit Funnel URL']").get_attribute(
                'href')
            soup = bs4.BeautifulSoup(sideSortList.get_attribute('innerHTML'), 'html.parser')
            funnelSteps = soup.findAll("div", {"class": "funnel_step"})
            browser.get(visitUrl)
            htmlSource = browser.page_source
            browser.close()
            time.sleep(5)
            browser.switch_to.window(browser.window_handles[-1])
            time.sleep(10)
            stepNameAr = []
            stepTypeAr = []

            for funnelStep in funnelSteps:
                stepName = funnelStep.find("div", {"class": "funnelSideStepPageTitle"}).get_text()
                stepType = funnelStep.find("div", {"class": "funnelSideStepPageViews"}).get_text()

                if stepName:
                    stepNameAr.append(stepName)

                if stepType:
                    stepTypeAr.append(stepType)

            stepNameAr = [x.replace('\n', '') for x in stepNameAr]
            stepNameAr = ','.join(map(str, stepNameAr))
            stepTypeAr = [x.replace('\n', '') for x in stepTypeAr]
            stepTypeAr = ','.join(map(str, stepTypeAr))

            print("Click funnel id >>>>>> ", funnelId)

            select_query = (
                    "select funnels_data.funnel_id from funnels_data where funnels_data.funnel_id = '%s'" % funnelId)
            resp_select = cursor.execute(select_query)
            rowcount = cursor.rowcount

            if rowcount > 0:
                update_query = (
                    "UPDATE funnels_data SET funnel_url = %s, html_data = %s, group_name = %s, funnel_name = %s, last_updated = %s, steps = %s, step_name = %s, step_type = %s, step_link = %s WHERE funnel_id = %s")
                update_values = (
                    visitUrl, htmlSource, groupName, funnelName, lastUpdated, steps, stepNameAr, stepTypeAr,
                    stepLinkUrl,
                    funnelId)
                resp = cursor.execute(update_query, update_values)
                connection.commit()
            else:
                insert_query = (
                    "INSERT INTO funnels_data (funnel_url, html_data, group_name, funnel_name, last_updated, steps, step_name, step_type, step_link, funnel_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)")
                insert_values = (
                    visitUrl, htmlSource, groupName, funnelName, lastUpdated, steps, stepNameAr, stepTypeAr,
                    stepLinkUrl,
                    funnelId)
                resp = cursor.execute(insert_query, insert_values)
                connection.commit()

    return True


t3 = PythonOperator(
    task_id='get_report_from_funnel',
    python_callable=get_report_from_funnel,
    provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='check_driver',
    python_callable=check_driver,
    provide_context=True,
    dag=dag)

t1 = PythonOperator(
    task_id='confirm_params',
    python_callable=confirm_parameters,
    dag=dag
)

t1 >> t2 >> t3
