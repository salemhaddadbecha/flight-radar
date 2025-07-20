from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import sys
import os
from transform.process_flights import calculate_top_active_flight_company
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

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

    kpi_1 = PythonOperator(
        task_id='kpi_1',
        python_callable=calculate_top_active_flight_company
    )
