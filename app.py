import os
import time
import json

from flask import Flask
from datetime import date
from flask import request

app = Flask(__name__)

BASE_PATH = "/home/ubuntu/docker-airflow/dags/"
BASE_PATH_KEYS = "/home/ubuntu/docker-airflow/keys/"


@app.route('/create_dag', methods=['GET'])
def create_dag():
    account_id = request.args.get('account_id')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account ID ", account_id)
    print("Details ID ", details)
    if account_id:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}act_{account_id}_daily_dag.py'.format(account_id=account_id, BASE_PATH=BASE_PATH)):

                file_name = "act_{account_id}_variables".format(account_id=account_id)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "act_{account_id}_dag_variables".format(account_id=account_id): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH, account_id=account_id,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))
                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "act_{account_id}_variables".format(account_id=account_id)
                variable_definition = {
                    "act_{account_id}_dag_variables".format(account_id=account_id): json.loads(details)
                }
                with open("facebook_daily_dag_template.py", 'r') as template_content:
                    ddd = date.today()
                    content = template_content.read().replace("START_DATE",
                                                              "datetime({year}, {month}, {day})".format(year=ddd.year,
                                                                                                        month=ddd.month,
                                                                                                        day=ddd.day))
                    content = content.replace('DAG_NAME',
                                              "act_{account_id}_daily_run_dag".format(account_id=account_id))
                    content = content.replace('VARIABLES_NAME',
                                              "act_{account_id}_dag_variables".format(account_id=account_id))
                    content = content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}act_{account_id}_daily_dag.py".format(BASE_PATH=BASE_PATH, account_id=account_id),
                          'w') as dag_file:
                    dag_file.write(content)

                with open("facebook_backlog_dag_template.py", 'r') as backlog_template_content:
                    # backlog_ddd = date.today() + timedelta(days=1)
                    backlog_ddd = date.today()
                    backlog_content = backlog_template_content.read().replace("START_DATE",
                                                                              "datetime({year}, {month}, {day})".format(
                                                                                  year=backlog_ddd.year,
                                                                                  month=backlog_ddd.month,
                                                                                  day=backlog_ddd.day))
                    backlog_content = backlog_content.replace('DAG_NAME',
                                                              "act_{account_id}_backlog_run_dag".format(
                                                                  account_id=account_id))
                    backlog_content = backlog_content.replace('VARIABLES_NAME',
                                                              "act_{account_id}_dag_variables".format(
                                                                  account_id=account_id))

                with open(
                        "{BASE_PATH}act_{account_id}_backlog_dag.py".format(BASE_PATH=BASE_PATH, account_id=account_id),
                        'w') as backlog_dag_file:
                    backlog_dag_file.write(backlog_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH, account_id=account_id,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))
                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)
                # time.sleep(3)
                # unpause_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                #     DAG_ID="act_{account_id}_daily_run_dag".format(account_id=account_id))
                # unpause_backlog_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                #     DAG_ID="act_{account_id}_backlog_run_dag".format(account_id=account_id))
                # os.system(unpause_dag)
                # time.sleep(1)
                # os.system(unpause_backlog_dag)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/act_{account_id}*""".format(
                account_id=account_id)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/act_{account_id}_variables.json""".format(
                account_id=account_id)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_stripe_dag', methods=['GET'])
def create_stripe_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}stripe_{account_name}_charge_dag.py'.format(account_name=account_name,
                                                                            BASE_PATH=BASE_PATH)):
                file_name = "stripe_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "stripe_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "stripe_{account_name}_variables".format(account_name=account_name)
                variable_definition = {
                    "stripe_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("stripe_account_charge_dag.py", 'r') as charge_template_content:
                    ddd = date.today()
                    charge_content = charge_template_content.read().replace("START_DATE",
                                                                            "datetime({year}, {month}, {day})".format(
                                                                                year=ddd.year,
                                                                                month=ddd.month,
                                                                                day=ddd.day))
                    charge_content = charge_content.replace('DAG_NAME',
                                                            "stripe_{account_name}_charge_dag".format(
                                                                account_name=account_name))
                    charge_content = charge_content.replace('VARIABLES_NAME',
                                                            "stripe_{account_name}_dag_variables".format(
                                                                account_name=account_name))
                    charge_content = charge_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}stripe_{account_name}_charge_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                  account_name=account_name),
                          'w') as charge_dag_file:
                    charge_dag_file.write(charge_content)

                with open("stripe_account_dispute_dag.py", 'r') as dispute_template_content:
                    ddd = date.today()
                    dispute_content = dispute_template_content.read().replace("START_DATE",
                                                                              "datetime({year}, {month}, {day})".format(
                                                                                  year=ddd.year,
                                                                                  month=ddd.month,
                                                                                  day=ddd.day))
                    dispute_content = dispute_content.replace('DAG_NAME',
                                                              "stripe_{account_name}_dispute_dag".format(
                                                                  account_name=account_name))
                    dispute_content = dispute_content.replace('VARIABLES_NAME',
                                                              "stripe_{account_name}_dag_variables".format(
                                                                  account_name=account_name))
                    dispute_content = dispute_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}stripe_{account_name}_dispute_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                   account_name=account_name),
                          'w') as dispute_dag_file:
                    dispute_dag_file.write(dispute_content)

                with open("stripe_account_refund_dag.py", 'r') as refund_template_content:
                    ddd = date.today()
                    refund_content = refund_template_content.read().replace("START_DATE",
                                                                            "datetime({year}, {month}, {day})".format(
                                                                                year=ddd.year,
                                                                                month=ddd.month,
                                                                                day=ddd.day))
                    refund_content = refund_content.replace('DAG_NAME',
                                                            "stripe_{account_name}_refund_dag".format(
                                                                account_name=account_name))
                    refund_content = refund_content.replace('VARIABLES_NAME',
                                                            "stripe_{account_name}_dag_variables".format(
                                                                account_name=account_name))
                    refund_content = refund_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}stripe_{account_name}_refund_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                  account_name=account_name),
                          'w') as refund_dag_file:
                    refund_dag_file.write(refund_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)
                time.sleep(3)
                unpause_charge_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                    DAG_ID="stripe_{account_name}_charge_dag".format(account_name=account_name))
                unpause_dispute_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                    DAG_ID="stripe_{account_name}_dispute_dag".format(account_name=account_name))
                unpause_refund_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                    DAG_ID="stripe_{account_name}_refund_dag".format(account_name=account_name))
                os.system(unpause_charge_dag)
                time.sleep(1)
                os.system(unpause_dispute_dag)
                time.sleep(1)
                os.system(unpause_refund_dag)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/stripe_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/stripe_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_shopify_dag', methods=['GET'])
def create_shopify_dag():
    shop_name = request.args.get('shop_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Shop Name ", shop_name)
    print("Details ID ", details)
    if shop_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}shopify_{shop_name}_transaction_dag.py'.format(shop_name=shop_name,
                                                                               BASE_PATH=BASE_PATH)):
                file_name = "shopify_{shop_name}_variables".format(shop_name=shop_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "shopify_{shop_name}_dag_variables".format(shop_name=shop_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "shopify_{shop_name}_variables".format(shop_name=shop_name)
                variable_definition = {
                    "shopify_{shop_name}_dag_variables".format(shop_name=shop_name): json.loads(details)
                }
                with open("shopify_account_dag.py", 'r') as transaction_template_content:
                    ddd = date.today()
                    transaction_content = transaction_template_content.read().replace("START_DATE",
                                                                                      "datetime({year}, {month}, {day})".format(
                                                                                          year=ddd.year,
                                                                                          month=ddd.month,
                                                                                          day=ddd.day))
                    transaction_content = transaction_content.replace('DAG_NAME',
                                                                      "shopify_{shop_name}_transaction_dag".format(
                                                                          shop_name=shop_name))
                    transaction_content = transaction_content.replace('VARIABLES_NAME',
                                                                      "shopify_{shop_name}_dag_variables".format(
                                                                          shop_name=shop_name))
                    transaction_content = transaction_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}shopify_{shop_name}_transaction_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                     shop_name=shop_name),
                          'w') as transaction_dag_file:
                    transaction_dag_file.write(transaction_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)
                time.sleep(3)
                unpause_charge_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow unpause {DAG_ID}""".format(
                    DAG_ID="shopify_{shop_name}_transaction_dag".format(shop_name=shop_name))
                os.system(unpause_charge_dag)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/shopify_{shop_name}*""".format(
                shop_name=shop_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/shopify_{shop_name}_variables.json""".format(
                shop_name=shop_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_klarna_dag', methods=['GET'])
def create_klarna_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}klarna_{account_name}_transaction_dag.py'.format(account_name=account_name,
                                                                                 BASE_PATH=BASE_PATH)):
                file_name = "klarna_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "klarna_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "klarna_{account_name}_variables".format(account_name=account_name)
                variable_definition = {
                    "klarna_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("klarna_account_dag.py", 'r') as transaction_template_content:
                    ddd = date.today()
                    transaction_content = transaction_template_content.read().replace("START_DATE",
                                                                                      "datetime({year}, {month}, {day})".format(
                                                                                          year=ddd.year,
                                                                                          month=ddd.month,
                                                                                          day=ddd.day))
                    transaction_content = transaction_content.replace('DAG_NAME',
                                                                      "klarna_{account_name}_transaction_dag".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('VARIABLES_NAME',
                                                                      "klarna_{account_name}_dag_variables".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}klarna_{account_name}_transaction_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                       account_name=account_name),
                          'w') as transaction_dag_file:
                    transaction_dag_file.write(transaction_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/klarna_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/klarna_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_paypal_dag', methods=['GET'])
def create_paypal_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}paypal_{account_name}_transaction_dag.py'.format(account_name=account_name,
                                                                                 BASE_PATH=BASE_PATH)):
                file_name = "paypal_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "paypal_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "paypal_{account_name}_variables".format(account_name=account_name)
                variable_definition = {
                    "paypal_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("paypal_account_dag.py", 'r') as transaction_template_content:
                    ddd = date.today()
                    transaction_content = transaction_template_content.read().replace("START_DATE",
                                                                                      "datetime({year}, {month}, {day})".format(
                                                                                          year=ddd.year,
                                                                                          month=ddd.month,
                                                                                          day=ddd.day))
                    transaction_content = transaction_content.replace('DAG_NAME',
                                                                      "paypal_{account_name}_transaction_dag".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('VARIABLES_NAME',
                                                                      "paypal_{account_name}_dag_variables".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}paypal_{account_name}_transaction_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                       account_name=account_name),
                          'w') as transaction_dag_file:
                    transaction_dag_file.write(transaction_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/paypal_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/paypal_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_authorize_dag', methods=['GET'])
def create_authorize_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}authorize_{account_name}_transaction_dag.py'.format(account_name=account_name,
                                                                                    BASE_PATH=BASE_PATH)):
                file_name = "authorize_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "authorize_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "authorize_{account_name}_variables".format(account_name=account_name)
                variable_definition = {
                    "authorize_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("authorize_account_dag.py", 'r') as transaction_template_content:
                    ddd = date.today()
                    transaction_content = transaction_template_content.read().replace("START_DATE",
                                                                                      "datetime({year}, {month}, {day})".format(
                                                                                          year=ddd.year,
                                                                                          month=ddd.month,
                                                                                          day=ddd.day))
                    transaction_content = transaction_content.replace('DAG_NAME',
                                                                      "authorize_{account_name}_transaction_dag".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('VARIABLES_NAME',
                                                                      "authorize_{account_name}_dag_variables".format(
                                                                          account_name=account_name))
                    transaction_content = transaction_content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")

                with open("{BASE_PATH}authorize_{account_name}_transaction_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                          account_name=account_name),
                          'w') as transaction_dag_file:
                    transaction_dag_file.write(transaction_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/authorize_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/authorize_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_google_dag', methods=['GET'])
def create_google_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}google_{account_name}_dag.py'.format(account_name=account_name, BASE_PATH=BASE_PATH)):

                file_name = "google_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "google_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH, account_name=account_name,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))
                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "google_{account_name}_variables".format(account_name=account_name)
                d = json.loads(details)
                variable_definition = {
                    "google_{account_name}_dag_variables".format(account_name=account_name): d
                }
                with open("googlead.yaml", 'r') as google_ad_yaml_content:
                    g_content = google_ad_yaml_content.read().replace("DEVELOPER_TOKEN", d['developer_token'])
                    g_content = g_content.replace("CLIENT_ID", d['client_id'])
                    g_content = g_content.replace("CLIENT_SECRET", d['client_secret'])
                    g_content = g_content.replace("REFRESH_TOKEN", d['refresh_token'])

                with open("{BASE_PATH}google_{account_name}_dag.yaml".format(BASE_PATH=BASE_PATH_KEYS,
                                                                             account_name=account_name),
                          'w') as dag_file:
                    dag_file.write(g_content)
                with open("google_account_dag.py", 'r') as google_template_content:
                    ddd = date.today()
                    content = google_template_content.read().replace("START_DATE",
                                                                     "datetime({year}, {month}, {day})".format(
                                                                         year=ddd.year,
                                                                         month=ddd.month,
                                                                         day=ddd.day))
                    content = content.replace('DAG_NAME',
                                              "google_{account_name}_dag".format(account_name=account_name))
                    content = content.replace('VARIABLES_NAME',
                                              "google_{account_name}_dag_variables".format(account_name=account_name))
                    content = content.replace('SCHEDULE_INTERVAL', "*/15 * * * *")
                    content = content.replace('YAML_FILE', "google_{account_name}_dag.yaml".format(
                        account_name=account_name))

                with open("{BASE_PATH}google_{account_name}_dag.py".format(BASE_PATH=BASE_PATH,
                                                                           account_name=account_name),
                          'w') as dag_file:
                    dag_file.write(content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH, account_name=account_name,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))
                time.sleep(1)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/google_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/google_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/keys/google_{account_name}_dag.yaml""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


@app.route('/create_clickfunnel_dag', methods=['GET'])
def create_clickfunnel_dag():
    account_name = request.args.get('account_name')
    details = request.args.get('details')
    event_type = request.args.get('event_type')
    print("Account Name ", account_name)
    print("Details ID ", details)
    if account_name:
        if event_type == "creation":
            if os.path.exists(
                    '{BASE_PATH}clickfunnel_{account_name}_dag.py'.format(account_name=account_name,
                                                                          BASE_PATH=BASE_PATH)):
                file_name = "clickfunnel_{account_name}_variables".format(account_name=account_name)
                remove_file_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/{file_name}.json""".format(
                    file_name=file_name)
                os.system(remove_file_command)
                time.sleep(1)
                variable_definition = {
                    "clickfunnel_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)
            else:
                file_name = "clickfunnel_{account_name}_variables".format(account_name=account_name)
                variable_definition = {
                    "clickfunnel_{account_name}_dag_variables".format(account_name=account_name): json.loads(details)
                }
                with open("click_funnel_dag.py", 'r') as clickfunnel_template_content:
                    ddd = date.today()
                    funnel_content = clickfunnel_template_content.read().replace("START_DATE",
                                                                                 "datetime({year}, {month}, {day})".format(
                                                                                     year=ddd.year,
                                                                                     month=ddd.month,
                                                                                     day=ddd.day))
                    funnel_content = funnel_content.replace('DAG_NAME',
                                                            "clickfunnel_{account_name}_dag".format(
                                                                account_name=account_name))
                    funnel_content = funnel_content.replace('VARIABLES_NAME',
                                                            "clickfunnel_{account_name}_dag_variables".format(
                                                                account_name=account_name))
                    funnel_content = funnel_content.replace('SCHEDULE_INTERVAL', "0 * * * *")

                with open("{BASE_PATH}clickfunnel_{account_name}_dag.py".format(BASE_PATH=BASE_PATH,
                                                                                account_name=account_name),
                          'w') as clickfunnel_dag_file:
                    clickfunnel_dag_file.write(funnel_content)

                with open("{BASE_PATH}config/{filename}.json".format(BASE_PATH=BASE_PATH,
                                                                     filename=file_name),
                          'w') as variable_file:
                    variable_file.write(json.dumps(variable_definition, indent=4))

                time.sleep(2)

                set_variable_command = """cd /home/ubuntu/docker-airflow &&  sudo docker-compose run --rm webserver airflow variables --import /usr/local/airflow/dags/config/{file_name}.json """.format(
                    file_name=file_name)
                os.system(set_variable_command)

                time.sleep(1)
                restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

        elif event_type == "deletion":
            deletion_command = """cd /home/ubuntu/docker-airflow && sudo rm -rf /home/ubuntu/docker-airflow/dags/clickfunnel_{account_name}*""".format(
                account_name=account_name)
            os.system(deletion_command)
            time.sleep(1)
            deletion_var_command = """cd /home/ubuntu/docker-airflow && sudo rm -f /home/ubuntu/docker-airflow/dags/config/clickfunnel_{account_name}_variables.json""".format(
                account_name=account_name)
            os.system(deletion_var_command)
            time.sleep(1)
            restart_command = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
            os.system(restart_command)

        return 'Request For Dag Creation has been sent'
    else:
        return 'No account id found'


if __name__ == '__main__':
    app.run(host='0.0.0.0')
