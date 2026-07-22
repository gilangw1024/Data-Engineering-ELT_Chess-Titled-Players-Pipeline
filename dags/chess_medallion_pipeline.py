import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# FIX PATH (disesuaikan)
BASE_DIR = Path(__file__).resolve().parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

# IMPORT YANG BENAR (tanpa prefix 'dags.')
from include.extract.extract import extract_chess_games
from include.transform.transform import transform_chess_data
from include.load.load import load_to_silver, load_to_gold

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data_engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def run_extract(**kwargs):
    ti = kwargs['ti']
    params = kwargs.get('params', {})
    username = params.get('username', 'hikaru')
    year = params.get('year', '2024')
    month = params.get('month', '01')
    
    logger.info(f"Starting extraction for {username} ({year}-{month})")
    result = extract_chess_games(username, year, month)
    
    ti.xcom_push(key='extract_meta', value=result)
    return result

def run_transform_and_load_silver(**kwargs):
    ti = kwargs['ti']
    extract_result = ti.xcom_pull(task_ids='extract_chess_data', key='extract_meta')
    if not extract_result:
        raise ValueError("Tidak ada metadata dari extract task")
        
    metadata = extract_result.get('metadata', {})
    username = metadata.get('username')
    year = metadata.get('year')
    month = metadata.get('month')
    
    if not all([username, year, month]):
        raise KeyError(f"Parameter wajib hilang. Metadata: {metadata}")
        
    logger.info(f"Starting transform + load silver for {username} ({year}-{month})")
    
    transform_result = transform_chess_data(username, year, month)
    
    validated_count = transform_result.get('validated_count', 0)
    logger.info(f"Loading {validated_count} records to Silver layer")
    load_result = load_to_silver(transform_result)
    
    xcom_safe_result = {
        'status': transform_result.get('status'),
        'validated_count': validated_count,
        'invalid_count': transform_result.get('invalid_count', 0),
        'load_status': load_result.get('status'),
        'inserted': load_result.get('inserted', 0),
        'metadata': transform_result.get('metadata'),
    }
    ti.xcom_push(key='transform_load_meta', value=xcom_safe_result)
    
    return xcom_safe_result

def run_load_gold(**kwargs):
    logger.info("Loading aggregated data to Gold layer")
    return load_to_gold()

with DAG(
    dag_id='chess_medallion_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    description='Chess Titled Players Analytics Pipeline',
    tags=['chess', 'medallion', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract_chess_data',
        python_callable=run_extract,
    )

    transform_silver = PythonOperator(
        task_id='transform_and_load_silver',
        python_callable=run_transform_and_load_silver,
    )

    gold = PythonOperator(
        task_id='load_to_gold',
        python_callable=run_load_gold,
    )

    extract >> transform_silver >> gold