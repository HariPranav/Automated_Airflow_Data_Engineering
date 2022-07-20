# Orchestration and Automation of Data Engineering Pipelines using Apache Airflow


![image](https://user-images.githubusercontent.com/28874545/180042983-b22de46f-4bc2-4b03-b88a-c44f51ea6600.png)

During the lifecycle of Data Engineering we come across a variety of tools which have different functionality to handle different data and data types. These tools often are independent of each other and if there are use cases which involve sending the output of table from a tool to a different table then, this involves the creation of scripts and cron jobs and the installation of KEYS and other authentication information which run on a compute resource. It can become tedious to maintain these scripts and rotation of KEYS to ensure the smooth automated running of data pipelines and workflows.

Hence in this blog we are going to be using AWS Managed Service of Apache Airflow as a serverless tool to create data pipelines to interact with SNOWFLAKE.

As per the architecture diagram, we have our Database in Snowflake, the queries are written in SQL in a Python script and the output of the table is again stored in the Snowflake Database. Once this script runs we can trigger another one to run, using the concept of **DAGS** which will be discussed further in this blog post.

We need to create an S3 bucket with the structure as shown below.

![image](https://user-images.githubusercontent.com/28874545/172366256-2d67c01b-1e3b-4184-b814-7fbaa87dbe67.png)

Here the **DAG** folder will contain the code responsible to run the different pipelines.

The **requirements.txt** file will contain the dependencies required for the script to run

## Connecting to Snowflake DB:

We need to install a few dependencies withing the requirements.txt file to connect to Snowflake. Open the requirements file and add the following dependencies.

    apache-airflow-providers-snowflake==1.3.0
    pandas
    snowflake-connector-python==2.5.1
    snowflake-sqlalchemy==1.2.5

Upload this file to the S3 bucket and this will automatically get synched with MWAA.

Create user Id and password in Snowflake so that we can use the same for connection.

We can now add a CONNECTION which can be used to directly run the scripts without hardcoding of variables.
This can be accessed as shown in the screenshot below

**ADMIN-> CONNECTION**

![image](https://user-images.githubusercontent.com/28874545/172369777-3864cfcf-5ef3-4fec-bfd4-0523fcec7605.png)

Although MWAA has inbuilt CONNECTION tab which can connect to multiple data sources, it has been observed that due to a bug the data will pass thorough the public internet rather than the private internet.

Hence we need to hard code the variables within our script and we will be discussing an efficient way to remove the hard coding of the variables in the blog.

Next Whitelist the MWAA IP address in Snowflake Console as given in the links below.

[Whitelist IP in Snowflake](https://docs.snowflake.com/en/user-guide/network-policies.html)

[Whitelist IP in Snowflake](https://docs.snowflake.com/en/sql-reference/functions/system_whitelist.html)

Once this is done we need to add the connection variables in as shown in the screenshot below.

Navigate to **ADMIN->VARIABLES**

Add the key value pairs(user name and passwords for snowflake user).

Create a file called **SCRIPT.py** and upload to **S3->DAGS** folder. Wait until this file is synched.

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

Then create a file called : **DAG.py** paste the code given below and upload to **S3->DAGS folder**

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

## Import Statements:

    from airflow import DAG
    from airflow.operators.bash import BashOperator

Here we are importing the **DAG** and **BASH** operators.
The **BASH** operator is used here to allow MWAA to run the script as a bash operation

## Connection To Snowflake:

    host = Variable.get("host")
    user = Variable.get("user")
    password= Variable.get("password")
    database = Variable.get("database")
    password_engine = Variable.get("password_engine")
    argument= host+' '+user+' '+password+' '+database+' '+password_engine
    print(host,user,password,database,password_engine)

Here we need to store the **Key Value Pairs** inside MWAA under the **ADMIN->VARIABLES**.

## Running the Script:

    dag = DAG('script', default_args=default_args)
    t1 = BashOperator(
    task_id='testairflow',
    bash_command='python3 /usr/local/airflow/dags/anuj_dag_script.py {argument}'.format(argument=argument),
    dag=dag)

After uploading all the files Run the DAG !.

If your DAG ran successfully then give yourself a pat on the back as you will now be able to create complex workflows such as :

1. Read the data from a data source to S3 using AWS DMS service.

2. Transform the data in S3 and store the data as tables using Athena

3. Push the data into another database for archival or to a visualization tool like Quicksight.

If you liked this blog, kindly share the same with your friends and Clap !!
