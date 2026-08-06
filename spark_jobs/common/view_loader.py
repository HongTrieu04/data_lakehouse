import os
from spark_jobs.common.io_utils import read_parquet

BRONZE_TABLE_MAPPING = [
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
    "bz_source_saoke_crb",
    "bz_t24core_stmt_entry",
    "bz_t24core_pd_payment_due_his_mv",
    "bz_t24core_pd_payment_due",
    "bz_t24core_company",
    "bz_pg_t24core_currency",
    "bz_pg_t24core_currency_his",
]

def load_all_bronze_views(spark, etl_date: str = "2026-08-06"):
    """Loads all ingested Bronze Parquet tables into temporary views for Spark SQL queries."""
    for trg in BRONZE_TABLE_MAPPING:
        path = f"s3a://bronze/{trg}/{etl_date}/"
        try:
            df = read_parquet(spark, path)
            df.createOrReplaceTempView(trg)
        except Exception as e:
            print(f"[WARN] Bronze view registration skipped for {trg}: {str(e)}")

def load_silver_view(spark, table_name: str, etl_date: str = "2026-08-06"):
    """Loads a Silver layer output Parquet into temporary views for downstream SQL queries."""
    path = f"s3a://silver/{table_name}/{etl_date}/"
    try:
        df = read_parquet(spark, path)
        df.createOrReplaceTempView(table_name)
    except Exception as e:
        print(f"[WARN] Silver view registration skipped for {table_name}: {str(e)}")
