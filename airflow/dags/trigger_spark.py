from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='spark_submit_example',
    schedule=None,
    default_args=default_args,
    catchup=False,
) as dag:

    submit_spark_job = SparkSubmitOperator(
        task_id='submit_spark_job',
        application='/opt/airflow/src/spark-test.py',  # Path to your Spark script
        conn_id='spark_default',                        # Spark connection ID in Airflow
        application_args=[],                            # Any arguments to your Spark app
        deploy_mode='client',  
    )
    
    submit_spark_job