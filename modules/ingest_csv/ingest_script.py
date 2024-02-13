import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import dotenv_values
import pandas as pd

def create_sql_statement(schema_name, table_name, df):
    
    sql_type_mapping = {
        'int64':'INTEGER',
        'float64':'FLOAT',
        'object':'VARCHAR',
        'datetime64':'TIMESTAMP'
    }
    
    sql_statement = f'CREATE TABLE {schema_name}.{table_name}_staging ('
    
    for column, dtype in df.dtypes.items():
        sql_type = sql_type_mapping.get(str(dtype), 'VARCHAR')
        sql_statement += f'{column} {sql_type}, '
        
    sql_statement = sql_statement[:-2]
    sql_statement += ')'
    
    return sql_statement 

def check_if_table_exists(cursor, schema_name, table_name):
    
    table_name = table_name.upper()
    schema_name = schema_name.upper()
    cursor.execute(
        f"""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = '{schema_name}'
        AND table_name = '{table_name}'
        """
    )
    
    if cursor.fetchone()[0] == 1:
        result_check = 1
    else: 
        result_check = 0
    
    return result_check

def upload_csv_to_snowflake(env_file, path, schema_name, table_name):
    
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
    key_column = df.columns[0]
    create_table_statement = create_sql_statement(schema_name = schema_name, table_name = table_name, df = df)
    
    try:
        cur.execute('USE ROLE ACCOUNTADMIN')
        cur.execute('USE DATABASE DBT_LEARN')
        cur.execute('USE SCHEMA {schema_name}'.format(schema_name = schema_name.upper()))
        cur.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name}_staging')
        cur.execute(create_table_statement)
        
        for index, row in df.iterrows():
            
            values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in row])
            
            cur.execute(
                f"""
                INSERT INTO {schema_name}.{table_name}_staging
                VALUES
                ({values})
                """
            )
        
        # write_pandas(snowflake_conn, df, table_name = table_name)
        
        result_check = check_if_table_exists(cursor = cur, schema_name = schema_name, table_name = table_name)
        
        if result_check == 1:
            cur.execute(f'DELETE FROM {schema_name}.{table_name} WHERE {key_column} IN (SELECT {key_column} FROM {schema_name}.{table_name}_staging)')
            cur.execute(f'INSERT INTO {schema_name}.{table_name} SELECT *, CURRENT_TIMESTAMP AS etl_date FROM {schema_name}.{table_name}_staging')
            cur.execute(f'DROP TABLE IF EXISTS {schema_name}.{table_name}_staging')
        
        else:
            cur.execute(f'ALTER TABLE {schema_name}.{table_name}_staging RENAME TO {table_name}')
            cur.execute(f'ALTER TABLE {schema_name}.{table_name} ADD COLUMN etl_date TIMESTAMP WITHOUT TIME ZONE')
            cur.execute(f'UPDATE {schema_name}.{table_name} SET etl_date = current_timestamp')
    
    except Exception as e:
        cur.close()
        snowflake_conn.close()
        print(f"Error occurred: {str(e)}")
    
    finally:
        cur.close()
    
    snowflake_conn.close()
    
    
