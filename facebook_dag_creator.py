from datetime import timedelta
from datetime import datetime, date
import time


def create_dag():
    with open("facebook_daily_dag_template.py", 'r') as template_content:
        ddd = date.today() + timedelta(days=1)
        content = template_content.read().replace("START_DATE", "datetime({year}, {month}, {day})".format(year=ddd.year, month=ddd.month, day=ddd.day))
        content = content.replace('DAG_NAME', "act_test_dag")
        content = content.replace('VARIABLES_NAME', "act_test_variables")
        content = content.replace('SCHEDULE_INTERVAL', '"*/15 * * * *"')

    with open("act_test_dag.py", 'w') as dag_file:
        dag_file.write(content)


create_dag()