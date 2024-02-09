from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import sys
import os
sys.path.append("/opt/airflow/modules/ingest_csv")
os.chdir('/opt/airflow/modules/ingest_csv')

from ingest_script import upload_csv_to_snowflake

@dag(
    dag_id = "Test_External_Connection_Snowflake",
    schedule_interval = "@once",
    tags = ["testing"],
    start_date = datetime(year=2023, month=6, day=1, hour=8, minute=00),
    max_active_runs = 1,
    catchup = False
)

def testing_ingest():
    
    create_table = PythonOperator(
        task_id = 'unload_to_snowflake',
        python_callable = upload_csv_to_snowflake,
        op_kwargs = {
            'env_file':'./.env',
            'path':'./data/test.csv',
            'table_name': 'public.test_table_staging',
            'table_ddl':'id int,name varchar(100)'
        }
    )
    
    insert_to_table_target = SnowflakeOperator(
        task_id = 'insert_to_table_target',
        snowflake_conn_id = 'snowflake_conn',
        sql = """
        insert into public.test_table (id, name)
        select * from public.test_table_staging;
        
        update public.test_table
        set etl_date = current_timestamp;
        
        drop table if exists public.test_table_staging;
        """
    )
    
    create_table >> insert_to_table_target
    
dag = testing_ingest()