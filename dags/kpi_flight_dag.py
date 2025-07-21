from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajout du path pour import des scripts
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from kpis.kpi1_company import calculate_top_active_flight_company
from kpis.kpi_2 import process_flights_by_continent
from kpis.kpi_3 import top_flight_distance
from kpis.kpi_4 import compute_flight_duration
from kpis.kpi_5 import get_active_company
from kpis.kpi_6 import get_top_models_by_country

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'kpi_flight_dag',
    default_args=default_args,
    description='DAG de calcul des KPIs FlightRadar',
    schedule_interval='0 */2 * * *',
    catchup=False,
    tags=['kpi', 'flights'],
) as dag:

    input_path = '/opt/airflow/data/Flights'
    output_path = '/opt/airflow/data/indicators'

    task_1 = PythonOperator(
        task_id='kpi_1_company',
        python_callable=calculate_top_active_flight_company,
        op_args=[input_path, output_path],
    )

    task_2 = PythonOperator(
        task_id='kpi_2_continent',
        python_callable=process_flights_by_continent,
        op_args=[input_path, output_path],
    )

    task_3 = PythonOperator(
        task_id='kpi_3_longest_flight',
        python_callable=top_flight_distance,
        op_args=[input_path, output_path],
    )

    task_4 = PythonOperator(
        task_id='kpi_8_most_common_model',
        python_callable=compute_flight_duration,
        op_args=[input_path, output_path],
    )

    task_5 = PythonOperator(
        task_id='kpi_12_longest_distance_by_country',
        python_callable=get_active_company,
        op_args=[input_path, output_path],
    )

    task_6 = PythonOperator(
        task_id='kpi_16_top_models_by_country',
        python_callable=get_top_models_by_country,
        op_args=[output_path],  # pas de input_path car il utilise l'API
    )

    # Orchestration
    task_1 >> task_2  >> task_3 >> task_3 >> task_4 >> task_5 >> task_6
