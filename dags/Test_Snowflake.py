from datetime import datetime
from airflow.decorators import dag
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

@dag(
    dag_id = "Test_Snowflake",
    schedule_interval = "@once",
    tags = ["testing"],
    start_date = datetime(year=2023, month=6, day=1, hour=8, minute=00),
    max_active_runs = 1,
    catchup = False
)

def testing():
    
    drop_table = SnowflakeOperator(
        task_id = 'drop_table',
        snowflake_conn_id = 'snowflake_conn',
        sql = """
        drop table if exists public.test_table;
        create table public.test_table (
            id int,
            name varchar(100),
            etl_date timestamp without time zone
        );
        """
    )
    
    insert_values = SnowflakeOperator(
        task_id = 'insert_values',
        snowflake_conn_id = 'snowflake_conn',
        sql = """
        insert into public.test_table (id, name)
        values
        (1, 'Iqbal'),
        (2, 'Effendy');
        """
    )
    
    update_etl_date = SnowflakeOperator(
        task_id = 'update_etl_date',
        snowflake_conn_id = 'snowflake_conn',
        sql = """
        update public.test_table
        set etl_date = current_timestamp
        """
    )
    
    drop_table >> insert_values >> update_etl_date
    
dag = testing()