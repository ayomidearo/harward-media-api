import os
import time
import json

from flask import Flask
from datetime import timedelta
from datetime import date
from flask import request

app = Flask(__name__)

BASE_PATH = "/home/ubuntu/docker-airflow/dags/"


# BASE_PATH = "/Users/arolambo/Code/harward-media-api/"


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
                    ddd = date.today() + timedelta(days=1)
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
                    #backlog_ddd = date.today() + timedelta(days=1)
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
                time.sleep(1)
                unpause_dag = """cd /home/ubuntu/docker-airflow &&  sudo docker restart docker-airflow_webserver_1 """
                os.system(restart_command)

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


if __name__ == '__main__':
    app.run(host='0.0.0.0')
