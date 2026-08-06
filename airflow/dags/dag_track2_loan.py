from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'fresher_2',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_track2_loan',
    default_args=default_args,
    description='Standalone Pipeline for Track 2 Loan Contract Domain (Tasks 9-12)',
    schedule='0 3 * * *',
    catchup=False,
    tags=['track2', 'loan_contract', 'fresher2'],
) as dag:

    # ----------------------------------------------------
    # PHASE 0: Oracle Source -> Landing / Bronze (21 Tables)
    # ----------------------------------------------------
    phase_0_oracle_to_bronze = BashOperator(
        task_id="phase_0_oracle_to_bronze_ingest",
        bash_command="python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{ ds }}"
    )

    # ----------------------------------------------------
    # PHASE 1: Build 6 Satellite Tables (Silver Satellite)
    # ----------------------------------------------------
    with TaskGroup("phase_1_silver_satellites") as phase_1:
        ar_bal = BashOperator(
            task_id="build_ar_bal",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/ar_bal.py {{ ds }}"
        )
        ar_rate_hist = BashOperator(
            task_id="build_ar_rate_hist",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/ar_rate_hist.py {{ ds }}"
        )
        ar_dlq_smy = BashOperator(
            task_id="build_ar_dlq_smy",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/ar_dlq_smy.py {{ ds }}"
        )
        ou = BashOperator(
            task_id="build_ou",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/ou.py {{ ds }}"
        )
        exg_rate = BashOperator(
            task_id="build_exg_rate",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/exg_rate.py {{ ds }}"
        )
        ast_ar_int_smy = BashOperator(
            task_id="build_ast_ar_int_smy",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/satellite/ast_ar_int_smy.py {{ ds }}"
        )

    # ----------------------------------------------------
    # PHASE 2 & PHASE 3: Build LOAN_AR (Task 10) & LOAN_AR_PRFL (Task 11)
    # ----------------------------------------------------
    with TaskGroup("phase_2_3_silver_temp2") as phase_2_3:
        loan_ar = BashOperator(
            task_id="build_loan_ar_task10",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/temp2_business/loan_ar.py {{ ds }}"
        )
        loan_ar_prfl = BashOperator(
            task_id="build_loan_ar_prfl_task11",
            bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/temp2_business/loan_ar_prfl.py {{ ds }}"
        )

    # ----------------------------------------------------
    # PHASE 4: Build INTF_LOAN_AR (Task 9 - Interface Table)
    # ----------------------------------------------------
    intf_loan_ar_task9 = BashOperator(
        task_id="build_intf_loan_ar_task9",
        bash_command="python /opt/airflow/spark_jobs/silver/track2_loan_contract/temp1_technical/intf_loan_ar.py {{ ds }}"
    )

    # ----------------------------------------------------
    # PHASE 5: Build DIM_LOAN_AR (Task 12 - Gold Dimension Table)
    # ----------------------------------------------------
    dim_loan_ar_task12 = BashOperator(
        task_id="build_dim_loan_ar_task12",
        bash_command="python /opt/airflow/spark_jobs/gold/track2_loan_contract/dim_loan_ar.py {{ ds }}"
    )

    # Execution Graph
    phase_0_oracle_to_bronze >> phase_1
    phase_0_oracle_to_bronze >> phase_2_3
    
    [phase_1, phase_2_3] >> intf_loan_ar_task9
    phase_2_3 >> dim_loan_ar_task12
