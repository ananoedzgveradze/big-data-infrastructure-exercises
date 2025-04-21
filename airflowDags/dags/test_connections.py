from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
import logging

def test_postgres():
    try:
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute('SELECT 1')
        result = cursor.fetchone()
        print(f"PostgreSQL Connection Test Result: {result}")
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"PostgreSQL Connection Test Failed: {str(e)}")
        raise

def test_s3():
    try:
        bucket_name = Variable.get('S3_BUCKET_NAME')
        s3_hook = S3Hook()  # Uses aws_default by default
        s3_client = s3_hook.get_conn()
        
        # Test if we can access the specific bucket
        response = s3_client.head_bucket(Bucket=bucket_name)
        print(f"S3 Connection Test - Successfully connected to bucket: {bucket_name}")
        
        # List a few objects in the bucket
        objects = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=5)
        if 'Contents' in objects:
            print(f"Sample objects in bucket: {[obj['Key'] for obj in objects['Contents']]}")
        else:
            print("Bucket is empty")
            
    except Exception as e:
        logging.error(f"S3 Connection Test Failed: {str(e)}")
        raise

with DAG(
    'test_connections',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['test']
) as dag:

    test_postgres_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres
    )

    test_s3_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3
    )

    test_postgres_task >> test_s3_task 