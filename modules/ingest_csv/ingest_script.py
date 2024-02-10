import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import dotenv_values
import pandas as pd

def create_sql_statement(table_name, df):
    sql_type_mapping = {
        'int64':'INTEGER',
        'float64':'FLOAT',
        'object':'VARCHAR',
        'datetime64':'TIMESTAMP'
    }
    
    sql_statement = 'CREATE TABLE {table_name} ('.format(table_name = table_name)
    
    for column, dtype in df.dtypes.items():
        sql_type = sql_type_mapping.get(str(dtype), 'VARCHAR')
        sql_statement += f'{column} {sql_type}, '
        
    sql_statement = sql_statement[:-2]
    sql_statement += ')'
    
    return sql_statement 

def upload_csv_to_snowflake(env_file, path, table_name):
    
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
    create_table_statement = create_sql_statement(table_name = table_name, df = df)
    
    try:
        cur.execute('USE ROLE ACCOUNTADMIN')
        cur.execute('USE DATABASE DBT_LEARN')
        cur.execute('DROP TABLE IF EXISTS {table_name}'.format(table_name = table_name))
        # cur.execute('CREATE TABLE {table_name} ({table_ddl})'.format(table_name = table_name, table_ddl = table_ddl))
        cur.execute(create_table_statement)
        
        for index, row in df.iterrows():
            cur.execute(
                """
                INSERT INTO {table_name}
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
    
    
