# To drop the schemas in Snowflake on which no queries have been run within a specified time, 
# excluding certain specified schemas
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('drop_schemas', default_args=default_args, schedule_interval='@daily')

time_period_days = Variable.get("drop_schemas_time_period_days", default_var=30)
excluded_schemas_str = Variable.get("drop_schemas_excluded_schemas", default_var='')
excluded_schemas = excluded_schemas_str.split(',') if excluded_schemas_str else []

drop_schemas_query = f"""
SELECT 'DROP SCHEMA IF EXISTS "' || schema_name || '";' AS drop_statement
FROM information_schema.schemata
WHERE schema_name NOT IN (
    SELECT DISTINCT TABLE_SCHEMA
    FROM information_schema.tables
    WHERE LAST_QUERY_ID IS NOT NULL
      AND LAST_QUERY_END_TIME >= CURRENT_TIMESTAMP() - INTERVAL '{time_period_days} DAY'
)
AND schema_name NOT IN ({', '.join([f"'{s}'" for s in excluded_schemas])})
"""

drop_schemas_task = SnowflakeOperator(
    task_id='drop_schemas',
    sql=drop_schemas_query,
    snowflake_conn_id='snowflake_connection',
    dag=dag
)

