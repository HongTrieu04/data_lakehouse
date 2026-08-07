import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.oracle_utils import read_oracle_table
from spark_jobs.common.io_utils import write_parquet, write_iceberg_table
from pyspark.sql import functions as F

SCHEMA_FOLDER_MAP = {
    "PG_T24CORE": "PG_T24CORE",
    "PG_FLEXBO": "PG_FLEXBO",
    "PG_LOS_APP": "PG_LOS",
    "PG_EBANKING": "PG_EBANKING",
    "PG_SOURCE": "PG_SAOKE"
}

# 21 Oracle Source Tables mapping definition for Track 2 (Tasks 9-12)
ORACLE_TRACK2_TABLES = [
    # Group A: Core tables for LOAN_AR / LOAN_AR_PRFL (14 tables)
    {"schema": "PG_T24CORE", "table": "LD_LOANS_AND_DEPOSITS", "target": "bz_t24core_ld_loans_and_deposits", "filter_col": "VALUE_DATE"},
    {"schema": "PG_T24CORE", "table": "LD_LOANS_AND_DEPOSITS_HIS", "target": "bz_t24core_ld_loans_and_deposits_his", "filter_col": None},
    {"schema": "PG_FLEXBO", "table": "PGB_LDTB_CONTRACT_MASTER", "target": "bz_flexbo_pgb_ldtb_contract_master", "filter_col": "BOOKING_DATE"},
    {"schema": "PG_FLEXBO", "table": "PGB_LOS_CONTRACT_FIELDS_TDATE", "target": "bz_flexbo_pgb_los_contract_fields_tdate", "filter_col": "MOV_DATE"},
    {"schema": "PG_LOS_APP", "table": "LOAN_DISBURSEMENT", "target": "bz_los_app_loan_disbursement", "filter_col": "BOOKING_DATE"},
    {"schema": "PG_LOS_APP", "table": "FACILITY", "target": "bz_los_app_facility", "filter_col": None},
    {"schema": "PG_LOS_APP", "table": "PRODUCT", "target": "bz_los_app_product", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "CUSTOMER", "target": "bz_t24core_customer", "filter_col": None},
    {"schema": "PG_EBANKING", "table": "COL_UDF_VALUE", "target": "bz_ebanking_col_udf_value", "filter_col": None},
    {"schema": "PG_FLEXBO", "table": "PGB_CONTRACT_UDF_MAP", "target": "bz_flexbo_pgb_contract_udf_map", "filter_col": None},
    {"schema": "PG_FLEXBO", "table": "PGBLD_CONTRACT_UDFIELD_HIST", "target": "bz_flexbo_pgbld_contract_udfield_hist", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "MB_MG_SAVING_MULTI", "target": "bz_t24core_mb_mg_saving_multi", "filter_col": None},
    {"schema": "PG_FLEXBO", "table": "PGBLD_RT_CONTRACT_UDFIELD_HIST", "target": "bz_flexbo_pgbld_rt_contract_udfield_hist", "filter_col": None},
    {"schema": "PG_SOURCE", "table": "SAOKE_MVMT", "target": "bz_source_saoke_mvmt", "filter_col": None},

    # Group B: Tables for 6 Satellite tables (7 tables)
    {"schema": "PG_SOURCE", "table": "SAOKE_CRB", "target": "bz_source_saoke_crb", "filter_col": "TXN_DATE"},
    {"schema": "PG_T24CORE", "table": "STMT_ENTRY", "target": "bz_t24core_stmt_entry", "filter_col": "BOOKING_DATE"},
    {"schema": "PG_T24CORE", "table": "PD_PAYMENT_DUE_HIS_MV", "target": "bz_t24core_pd_payment_due_his_mv", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "PD_PAYMENT_DUE", "target": "bz_t24core_pd_payment_due", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "COMPANY", "target": "bz_t24core_company", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "CURRENCY", "target": "bz_pg_t24core_currency", "filter_col": None},
    {"schema": "PG_T24CORE", "table": "CURRENCY_HIS", "target": "bz_pg_t24core_currency_his", "filter_col": None},
]

def ingest_oracle_to_bronze(etl_date: str = "2026-08-06", target_table: str = None):
    spark = get_spark(f"Oracle_To_Bronze_Track2_{etl_date}")
    print(f"[ORACLE INGEST] Starting Ingestion for ETL Date: {etl_date}")

    tables_to_process = ORACLE_TRACK2_TABLES
    if target_table:
        tables_to_process = [t for t in ORACLE_TRACK2_TABLES if t["target"] == target_table or t["table"] == target_table]

    for item in tables_to_process:
        src_table = f"{item['schema']}.{item['table']}"
        trg_table = item["target"]
        filter_col = item["filter_col"]
        schema_folder = SCHEMA_FOLDER_MAP.get(item["schema"], "PG_T24CORE")
        
        print(f"[ORACLE -> BRONZE] Reading Oracle table: {src_table}")
        try:
            # Read from Oracle via JDBC
            df_src = read_oracle_table(spark, src_table, etl_date=etl_date if filter_col else None, filter_col=filter_col)
            
            # Add Bronze standard audit columns
            df_bronze = df_src \
                .withColumn("BRZ_LOAD_DT", F.current_timestamp()) \
                .withColumn("SRC_SYSTEM", F.lit(item["schema"])) \
                .withColumn("ETL_BATCH_ID", F.lit(etl_date))

            # Save to MinIO Bronze bucket as Iceberg with Incremental Append
            write_iceberg_table(df_bronze, bucket="bronze", schema_name=schema_folder, table_name=trg_table, mode="append")

            print(f"[SUCCESS] Successfully ingested {src_table} -> Iceberg bronze/{schema_folder}/{trg_table.upper()}")
        except Exception as e:
            print(f"[ERROR] Failed to ingest {src_table}: {str(e)}")
            raise e
        except Exception as e:
            print(f"[ERROR] Failed to ingest {src_table}: {str(e)}")
            raise e

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    tbl = sys.argv[2] if len(sys.argv) > 2 else None
    ingest_oracle_to_bronze(etl_dt, tbl)
