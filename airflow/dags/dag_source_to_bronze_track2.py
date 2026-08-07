# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.utils.task_group import TaskGroup

# default_args = {
#     'owner': 'fresher_2',
#     'depends_on_past': False,
#     'start_date': datetime(2026, 8, 1),
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# with DAG(
#     dag_id='dag_source_to_bronze_track2',
#     default_args=default_args,
#     description='Oracle Source to Bronze Extraction for Track 2 Loan Contract Domain (21 Tables)',
#     schedule='0 2 * * *',  # Run daily at 02:00 AM
#     catchup=False,
#     max_active_tasks=2,    # Giới hạn tối đa 2 task Spark chạy đồng thời để bảo vệ RAM/CPU
#     tags=['oracle', 'bronze', 'track2', 'loan_contract', 'fresher2'],
# ) as dag:

#     # TaskGroup A: Extract Core Loan Contract Source Tables (14 Tables - Chạy tuần tự)
#     with TaskGroup("extract_oracle_group_a_core_tables") as group_a:
#         tables_group_a = [
#             "bz_t24core_ld_loans_and_deposits",
#             "bz_t24core_ld_loans_and_deposits_his",
#             "bz_flexbo_pgb_ldtb_contract_master",
#             "bz_flexbo_pgb_los_contract_fields_tdate",
#             "bz_los_app_loan_disbursement",
#             "bz_los_app_facility",
#             "bz_los_app_product",
#             "bz_t24core_customer",
#             "bz_ebanking_col_udf_value",
#             "bz_flexbo_pgb_contract_udf_map",
#             "bz_flexbo_pgbld_contract_udfield_hist",
#             "bz_t24core_mb_mg_saving_multi",
#             "bz_flexbo_pgbld_rt_contract_udfield_hist",
#             "bz_source_saoke_mvmt",
#         ]

#         prev_task = None
#         for tbl in tables_group_a:
#             curr_task = BashOperator(
#                 task_id=f"ingest_{tbl}",
#                 bash_command=f"python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{{{ ds }}}} {tbl}"
#             )
#             if prev_task:
#                 prev_task >> curr_task
#             prev_task = curr_task

#     # TaskGroup B: Extract Satellite Source Tables (7 Tables - Chạy tuần tự)
#     with TaskGroup("extract_oracle_group_b_satellites") as group_b:
#         tables_group_b = [
#             "bz_source_saoke_crb",
#             "bz_t24core_stmt_entry",
#             "bz_t24core_pd_payment_due_his_mv",
#             "bz_t24core_pd_payment_due",
#             "bz_t24core_company",
#             "bz_pg_t24core_currency",
#             "bz_pg_t24core_currency_his",
#         ]

#         prev_task = None
#         for tbl in tables_group_b:
#             curr_task = BashOperator(
#                 task_id=f"ingest_{tbl}",
#                 bash_command=f"python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{{{ ds }}}} {tbl}"
#             )
#             if prev_task:
#                 prev_task >> curr_task
#             prev_task = curr_task

#     # Chạy tuần tự: Group A xong toàn bộ mới sang Group B
#     group_a >> group_b

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
    max_active_tasks=3,    # Cho phép tối đa 3 task Spark chạy đồng thời (2 nhánh A + 1 nhánh B)
    tags=['oracle', 'bronze', 'track2', 'loan_contract', 'fresher2'],
) as dag:

    def build_sequential_chain(tables, prefix=""):
        """Tạo 1 chuỗi task chạy tuần tự từ list bảng, trả về task đầu và task cuối."""
        prev_task = None
        first_task = None
        for tbl in tables:
            curr_task = BashOperator(
                task_id=f"ingest_{tbl}",
                bash_command=f"python /opt/airflow/spark_jobs/landing/track2_loan_contract/oracle_to_bronze_track2.py {{{{ ds }}}} {tbl}"
            )
            if prev_task:
                prev_task >> curr_task
            else:
                first_task = curr_task
            prev_task = curr_task
        return first_task, prev_task  # (task đầu, task cuối)

    # TaskGroup A: Extract Core Loan Contract Source Tables (14 Tables)
    # Chia làm 2 nhánh con chạy SONG SONG, mỗi nhánh tuần tự bên trong
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

        # Chia đôi list 14 bảng -> 2 nhánh 7 bảng, chạy song song với nhau
        mid = len(tables_group_a) // 2
        tables_a1 = tables_group_a[:mid]
        tables_a2 = tables_group_a[mid:]

        with TaskGroup("branch_a1") as branch_a1:
            build_sequential_chain(tables_a1)

        with TaskGroup("branch_a2") as branch_a2:
            build_sequential_chain(tables_a2)

        # KHÔNG set dependency giữa branch_a1 và branch_a2 -> chạy song song

    # TaskGroup B: Extract Satellite Source Tables (7 Tables - Chạy tuần tự)
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

        build_sequential_chain(tables_group_b)

    # KHÔNG set dependency giữa group_a và group_b -> chạy song song luôn
    # Tổng cộng lúc cao điểm: branch_a1 (1 task) + branch_a2 (1 task) + group_b (1 task) = 3 task song song