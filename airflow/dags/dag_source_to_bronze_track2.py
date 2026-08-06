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
    dag_id='dag_source_to_bronze_track2',
    default_args=default_args,
    description='Oracle Source to Bronze Extraction for Track 2 Loan Contract Domain (21 Tables)',
    schedule='0 2 * * *',  # Run daily at 02:00 AM
    catchup=False,
    tags=['oracle', 'bronze', 'track2', 'loan_contract', 'fresher2'],
) as dag:

    # TaskGroup A: Extract Core Loan Contract Source Tables (14 Tables)
    with TaskGroup("extract_oracle_group_a_core_tables") as group_a:
        tables_group_a = [
            "bz_t24core_ld_loans_and_deposits",
            "bz_t24core_ld_loans_and_deposits_his",
            "bz_flexbo_pgb_ldtb_contract_master",
            "bz_flexbo_pgb_los_contract_fields_tdate",
            "bz_los_app_loan_disbursement",
            "bz_los_app_facility",
            "bz_los_app_product",
            "bz_t24core_customer",
            "bz_ebanking_col_udf_value",
            "bz_flexbo_pgb_contract_udf_map",
            "bz_flexbo_pgbld_contract_udfield_hist",
            "bz_t24core_mb_mg_saving_multi",
            "bz_flexbo_pgbld_rt_contract_udfield_hist",
            "bz_source_saoke_mvmt",
        ]

        for tbl in tables_group_a:
            BashOperator(
                task_id=f"ingest_{tbl}",
                bash_command=f"python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{{{ ds }}}} {tbl}"
            )

    # TaskGroup B: Extract Satellite Source Tables (7 Tables)
    with TaskGroup("extract_oracle_group_b_satellites") as group_b:
        tables_group_b = [
            "bz_source_saoke_crb",
            "bz_t24core_stmt_entry",
            "bz_t24core_pd_payment_due_his_mv",
            "bz_t24core_pd_payment_due",
            "bz_t24core_company",
            "bz_pg_t24core_currency",
            "bz_pg_t24core_currency_his",
        ]

        for tbl in tables_group_b:
            BashOperator(
                task_id=f"ingest_{tbl}",
                bash_command=f"python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{{{ ds }}}} {tbl}"
            )
