import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable

host = Variable.get("host")
user = Variable.get("user")
password= Variable.get("password")
database = Variable.get("database")
password_engine = Variable.get("password_engine")
argument= host+' '+user+' '+password+' '+database+' '+password_engine
print(host,user,password,database,password_engine)

seven_days_ago = datetime.combine(datetime.today() - timedelta(7),
                                datetime.min.time())

default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),

}

dag = DAG('script', default_args=default_args)
t1 = BashOperator(
task_id='testairflow',
bash_command='python3 /usr/local/airflow/dags/script.py {argument}'.format(argument=argument),
dag=dag)
