import os, sys
from datetime import datetime

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(PROJECT_DIR))

from airflow import DAG # type: ignore
from airflow.operators.python import PythonVirtualenvOperator, ShortCircuitOperator # type: ignore
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

from download_wfs_dags.check_wfs_download_config import check_wfs_download_config
from download_wfs_dags.task_2_wfs_download import task_2_wfs_download
from download_wfs_dags.task_3_insert_shapefile import task_3_insert_shape_file
from download_wfs_dags.task_4_check_if_trigger import task_4_check_if_trigger

requirements = ['geopandas', 'geoalchemy2']
    
venv_path = f"/opt/airflow/venv/inpe/download_wfs/venv"

with DAG(f'download_wfs_dag',
    schedule_interval='@daily',
    default_args={'start_date': datetime(2025, 1, 1),},
    tags=['download', 'wfs'],
    max_active_runs=1,
    catchup=False) as dag:
    
    task_1 = PythonVirtualenvOperator(
        task_id="1_check_config",
        requirements=requirements,
        python_callable=check_wfs_download_config,
        system_site_packages=True,
        op_kwargs={"project_dir": PROJECT_DIR},
        venv_cache_path=venv_path
    )
    
    task_2 = PythonVirtualenvOperator(
        task_id="2_wfs_download",
        requirements=requirements,
        python_callable=task_2_wfs_download,
        system_site_packages=True,
        op_kwargs={"project_dir": PROJECT_DIR, "logical_date": "{{ logical_date }}"},
        venv_cache_path=venv_path
    )
    
    task_3 = PythonVirtualenvOperator(
        task_id="3_insert_shape_file",
        requirements=requirements,
        python_callable=task_3_insert_shape_file,
        system_site_packages=True,
        op_kwargs={"project_dir": PROJECT_DIR},
        venv_cache_path=venv_path
    )
    
    task_4 = ShortCircuitOperator(
        task_id="4_check_if_trigger",
        python_callable=task_4_check_if_trigger,
        op_kwargs={"project_dir": PROJECT_DIR},
    )  
    
    trigger_self = TriggerDagRunOperator(
        task_id='trigger_self',
        trigger_dag_id=f'download_wfs_dag',
        conf={"message": "Rescheduling the DOWNLOAD_WFS for the next cycle."},
    )
    
    task_1  >> task_2 >> task_3 >> task_4
    task_4 >> trigger_self