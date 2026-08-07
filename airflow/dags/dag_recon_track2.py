from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'fresher_2',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_recon_track2',
    default_args=default_args,
    description='Dedicated Data Reconciliation DAG for Track 2 (Audits Oracle, Bronze, Silver, and Gold Layers)',
    schedule=None,
    catchup=False,
    max_active_tasks=1,
    tags=['track2', 'reconcile', 'audit', 'fresher2'],
) as dag:

    recon_task = BashOperator(
        task_id="run_track2_reconciliation",
        bash_command="python /opt/airflow/spark_jobs/recon/recon_track2_job.py {{ ds | default('2026-08-07') }}"
    )
