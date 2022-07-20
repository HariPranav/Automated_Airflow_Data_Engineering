import logging
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="Snowflake_Conn", default_args=args, schedule_interval=None
)

create_insert_query = [
    """create table public.test_airflow (amount number);""",
    """insert into public.test_airflow values(1),(2),(3);""",
]


def row_count(**context):
    dwh_hook = SnowflakeHook(snowflake_conn_id="Snowflake_Connection")
    result = dwh_hook.get_first("select count(*) from public.test_airflow")
    logging.info("Number of rows in `public.test_airflow`  - %s", result[0])


with dag:
    create_insert = SnowflakeOperator(
        task_id="snowfalke_create",
        sql=create_insert_query,
        snowflake_conn_id="Snowflake_Connection",
    )

    get_count = PythonOperator(task_id="get_count", python_callable=row_count)
create_insert >> get_count
