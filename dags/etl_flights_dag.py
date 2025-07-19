from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'flightradar_etl',
    default_args=default_args,
    description='Pipeline ETL FlightRadar24',
    schedule_interval='0 */2 * * *',  # toutes les 2 heures
    start_date=datetime(2025, 7, 1),
    catchup=False
) as dag:

    extract = BashOperator(
        task_id='extract_flights',
        bash_command='python /path/to/extract/extract_flights.py'
    )

    transform = BashOperator(
        task_id='process_flights',
        bash_command='python /path/to/transform/process_flights_spark.py /path/to/latest.csv'
    )

    extract >> transform
