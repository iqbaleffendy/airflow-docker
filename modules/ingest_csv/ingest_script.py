import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import dotenv_values
import pandas as pd

def upload_csv_to_snowflake(env_file, path, table_name, table_ddl):
    
    snowflake_cred = dotenv_values(env_file)
    snowflake_conn = snowflake.connector.connect(
        user = snowflake_cred['SNOWFLAKE_USERNAME'],
        password = snowflake_cred['SNOWFLAKE_PASSWORD'],
        account = snowflake_cred['SNOWFLAKE_ACCOUNT'],
        warehouse = snowflake_cred['SNOWFLAKE_WAREHOUSE'],
        database = snowflake_cred['SNOWFLAKE_DATABASE']
    )
    
    cur = snowflake_conn.cursor()
    
    df = pd.read_csv(path, sep=';')
    print(df)
    
    try:
        cur.execute('USE ROLE ACCOUNTADMIN')
        cur.execute('USE DATABASE DBT_LEARN')
        cur.execute('DROP TABLE IF EXISTS {table_name}'.format(table_name = table_name))
        cur.execute('CREATE TABLE {table_name} ({table_ddl})'.format(table_name = table_name, table_ddl = table_ddl))
        
        for index, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO {table_name} (id, name)
                VALUES
                ({id}, '{values}')
                """.format(table_name = table_name, id = row['id'], values = row['name'])
            )
        
        # write_pandas(snowflake_conn, df, table_name = 'public.test_table_staging')
    
    except:
        cur.close()
        snowflake_conn.close()
        print('Error')
    
    finally:
        cur.close()
    
    snowflake_conn.close()
    
    
