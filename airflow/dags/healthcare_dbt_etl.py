from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import os
import sys
# from dbt_airflow.core.config import DbtAirflowConfig, DbtProfileConfig, DbtProjectConfig
# from dbt_airflow.core.task_group import DbtTaskGroup
# from dbt_airflow.operators.execution import ExecutionOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from scripts import extraction
from scripts import loading


# BRONZE_PATH='/home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt/models/bronze'
# SILVER_PATH='/home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt/models/silver'
# GOLD_PATH='/home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt/models/gold'


# def get_dbt_models(layer_path):
#     return [f.replace(".sql", "") for f in os.listdir(layer_path) if f.endswith(".sql")]

# bronze_models = get_dbt_models(BRONZE_PATH)
# silver_models = get_dbt_models(SILVER_PATH)
# gold_models = get_dbt_models(GOLD_PATH)


# def create_dbt_task(model_name):
#     return BashOperator(
#         task_id=model_name,
#         bash_command=f"cd /home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt && dbt run --select {model_name}"
#     )

# bronze_tasks = [create_dbt_task(m) for m in bronze_models]
# silver_tasks = [create_dbt_task(m) for m in silver_models]
# gold_tasks = [create_dbt_task(m) for m in gold_models]

def extract():
    extraction.extraction()

def load():
    conn=loading.snow_configuratin()
    loading.main(conn)

    

with DAG (
    dag_id='etl_dbt_pipeline',
    start_date=datetime(2025, 10, 1),
    schedule='@daily',
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    tags=['etl', 'dbt']
) as dag:
    
    local_to_s3_extraction =PythonOperator(
        task_id='extraction_data',
        python_callable=extract
    )

    s3_to_snowflake_loading =PythonOperator(
        task_id='loading_data',
        python_callable=load
    )

    transform = BashOperator(
        task_id='Transformation',
        bash_command=f"cd /home/vivekpanthi/DataEngineering/Health_Care_ETL/healthcare_dbt && dbt run"

    )



local_to_s3_extraction>>s3_to_snowflake_loading>>transform

# for silver_task in silver_tasks:
#     silver_task.set_upstream(bronze_tasks)

# for gold_task in gold_tasks:
#     gold_task.set_upstream(silver_tasks)