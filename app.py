from flask import Flask
from datetime import timedelta
from datetime import date

app = Flask(__name__)

BASE_PATH = "/home/ubuntu/docker-airflow/dags/"


@app.route('/create_dag')
def hello_world():
    with open("facebook_daily_dag_template.py", 'r') as template_content:
        ddd = date.today() + timedelta(days=1)
        content = template_content.read().replace("START_DATE", "datetime({year}, {month}, {day})".format(year=ddd.year, month=ddd.month, day=ddd.day))
        content = content.replace('DAG_NAME', "act_test_dag")
        content = content.replace('VARIABLES_NAME', "act_test_variables")
        content = content.replace('SCHEDULE_INTERVAL', '"*/15 * * * *"')

    with open("{}act_test_dag.py".format(BASE_PATH), 'w') as dag_file:
        dag_file.write(content)

    with open("facebook_backlog_dag_template.py", 'r') as backlog_template_content:
        backlog_ddd = date.today() + timedelta(days=1)
        backlog_content = backlog_template_content.read().replace("START_DATE", "datetime({year}, {month}, {day})".format(year=backlog_ddd.year, month=backlog_ddd.month, day=backlog_ddd.day))
        backlog_content = backlog_content.replace('DAG_NAME', "act_test_dag")
        backlog_content = backlog_content.replace('VARIABLES_NAME', "act_test_variables")

    with open("{}act_test_backlog_dag.py".format(BASE_PATH), 'w') as backlog_dag_file:
        backlog_dag_file.write(backlog_content)

    return 'Request has been sent'


if __name__ == '__main__':
    app.run()
