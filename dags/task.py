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
drop_schemas_query = f"""
SELECT 'DROP SCHEMA IF EXISTS "' || schema_name || '";' AS drop_statement
FROM information_schema.schemata
WHERE schema_name NOT IN (
    SELECT DISTINCT TABLE_SCHEMA 
    FROM information_schema.tables
    WHERE LAST_QUERY_ID IS NOT NULL
      AND LAST_QUERY_END_TIME >= CURRENT_TIMESTAMP() - INTERVAL '{time_period_days} DAY'
)
"""

drop_schemas_task = SnowflakeOperator(
    task_id='drop_schemas',
    sql=drop_schemas_query,
    snowflake_conn_id='snowflake_connection',
    dag=dag
)
