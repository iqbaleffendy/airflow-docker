from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import date

current_month = date.today().strftime('%Y-%m')

@dag(
    dag_id = "DEV_Goalscorer_Pos_Segmentation_DBT",
    schedule_interval = "@once",
    tags = ["Pos_Segmentation"],
    start_date = datetime(year=2023, month=6, day=1, hour=8, minute=00),
    max_active_runs = 1,
    params = {'month_year': current_month},
    catchup = False
)


def pos_segmentation():
    
    start = EmptyOperator(task_id = "start")
    
    test_models = BashOperator(
        task_id = 'test_models',
        bash_command = """
        cd /opt/airflow/modules/test_dbt/ && \
        dbt test
        """
    )
    
    generate_table = BashOperator(
        task_id = 'generate_table',
        bash_command = """
        cd /opt/airflow/modules/test_dbt/ && \
        dbt run --select '+pos_segmentation_stg_final' --vars '{"month_year":"{{params.month_year}}"}'
        """
    )
    
    incremental_process = BashOperator(
        task_id = 'incremental_process',
        bash_command = """
        cd /opt/airflow/modules/test_dbt/ && \
        dbt run --select '1+pos_segmentation_update_diamond' --vars '{"month_year":"{{params.month_year}}"}'
        """
    )
    
    drop_stg_table = BashOperator(
        task_id = 'drop_stg_table',
        bash_command = """
        cd /opt/airflow/modules/test_dbt/ && \
        dbt run-operation drop_staging_tables --args '{"table_prefix": "pos_segmentation", "dryrun": False}'
        """
    )

    start >> generate_table >> test_models >> incremental_process >> drop_stg_table
    
dag = pos_segmentation() 