import sys
import os
from datetime import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.view_loader import load_all_bronze_views, load_silver_view
from spark_jobs.common.io_utils import read_parquet, write_iceberg_table
from spark_jobs.common.recon_framework import Track2Reconciler

def run_track2_reconciliation(etl_date: str = "2026-08-07"):
    print(f"============================================================")
    print(f"[RECON TRACK 2] Starting Data Reconciliation for ETL Date: {etl_date}")
    print(f"============================================================")

    spark = get_spark(f"Recon_Track2_{etl_date}")
    reconciler = Track2Reconciler(spark, etl_date)

    # 1. Load Views
    load_all_bronze_views(spark, etl_date)
    load_silver_view(spark, "LOAN_AR", etl_date)
    load_silver_view(spark, "LOAN_AR_PRFL", etl_date)
    load_silver_view(spark, "INTF_LOAN_AR", etl_date)
    load_silver_view(spark, "AR_BAL", etl_date)
    load_silver_view(spark, "AR_DLQ_SMY", etl_date)
    load_silver_view(spark, "AR_RATE_HIST", etl_date)
    load_silver_view(spark, "AST_AR_INT_SMY", etl_date)
    load_silver_view(spark, "EXG_RATE", etl_date)
    load_silver_view(spark, "OU", etl_date)

    try:
        df_dim = read_parquet(spark, "s3a://gold/DIM_LOAN_AR/data/")
        df_dim.createOrReplaceTempView("DIM_LOAN_AR")
    except Exception:
        print("[WARN] Could not load DIM_LOAN_AR for reconciliation.")
        df_dim = None

    # Retrieve Temp Views as DataFrames
    df_bz_ld = spark.table("bz_t24core_ld_loans_and_deposits")
    df_bz_flex = spark.table("bz_flexbo_pgb_ldtb_contract_master")
    df_bz_cus = spark.table("bz_t24core_customer")
    df_loan_ar = spark.table("LOAN_AR")
    df_loan_ar_prfl = spark.table("LOAN_AR_PRFL")
    df_intf = spark.table("INTF_LOAN_AR")
    df_ar_bal = spark.table("AR_BAL")
    df_ast_int = spark.table("AST_AR_INT_SMY")

    # --- STAGE 1: BRONZE KEY NON-NULL AUDITS ---
    print("\n--- [STAGE 1: BRONZE KEY INTEGRITY AUDIT] ---")
    reconciler.audit_null_keys(df_bz_ld, "RECID", "STAGE_1_BRONZE")
    reconciler.audit_null_keys(df_bz_flex, "CONTRACT_REF_NO", "STAGE_1_BRONZE")
    reconciler.audit_null_keys(df_bz_cus, "RECID", "STAGE_1_BRONZE")

    # --- STAGE 2: BRONZE -> SILVER TEMP 2 (LOAN_AR & LOAN_AR_PRFL) ---
    print("\n--- [STAGE 2: BRONZE -> SILVER TEMP 2 AUDIT] ---")
    reconciler.audit_null_keys(df_loan_ar, "AR_ID", "STAGE_2_SILVER_LOAN_AR")
    reconciler.audit_null_keys(df_loan_ar_prfl, "AR_ID", "STAGE_2_SILVER_LOAN_AR_PRFL")
    
    # Audit distinct contract counts
    df_loan_ar_dist = df_loan_ar.select("AR_ID").distinct()
    df_loan_ar_prfl_dist = df_loan_ar_prfl.select("AR_ID").distinct()
    reconciler.audit_row_count(df_loan_ar_dist, df_loan_ar_prfl_dist, "STAGE_2_SILVER_TEMP2", "LOAN_AR_VS_PRFL_DISTINCT_AR_ID")
    reconciler.audit_minus_test(df_loan_ar, df_loan_ar_prfl, "AR_ID", "AR_ID", "STAGE_2_SILVER_TEMP2")

    # --- STAGE 3: SILVER SATELITES & TEMP 2 -> SILVER TEMP 1 (INTF_LOAN_AR) ---
    print("\n--- [STAGE 3: SILVER TEMP 2 -> SILVER TEMP 1 INTF_LOAN_AR AUDIT] ---")
    intf_key = "AR_CODE" if "AR_CODE" in df_intf.columns else "AR_ID"
    df_intf_dist = df_intf.select(intf_key).distinct()
    reconciler.audit_row_count(df_loan_ar_dist, df_intf_dist, "STAGE_3_SILVER_INTF", "LOAN_AR_VS_INTF_LOAN_AR_UNIQUE_CONTRACTS")
    reconciler.audit_null_keys(df_intf, intf_key, "STAGE_3_SILVER_INTF")

    if "AR_BAL_LCY" in df_intf.columns and "AR_BAL_LCY" in df_ar_bal.columns:
        reconciler.audit_sum_metric(df_ar_bal, df_intf, "AR_BAL_LCY", "AR_BAL_LCY", "STAGE_3_SILVER_INTF", "BALANCE_SUM_AR_BAL_VS_INTF")

    # --- STAGE 4: SILVER TEMP 1 (INTF_LOAN_AR) -> GOLD (DIM_LOAN_AR) ---
    print("\n--- [STAGE 4: INTF_LOAN_AR -> GOLD DIM_LOAN_AR AUDIT] ---")
    if df_dim is not None:
        dim_key = "AR_CODE" if "AR_CODE" in df_dim.columns else "AR_ID"
        df_dim_dist = df_dim.select(dim_key).distinct()
        reconciler.audit_row_count(df_intf_dist, df_dim_dist, "STAGE_4_GOLD_DIM", "INTF_VS_GOLD_ROW_COUNT")
        reconciler.audit_null_keys(df_dim, dim_key, "STAGE_4_GOLD_DIM")
        reconciler.audit_minus_test(df_intf, df_dim, intf_key, dim_key, "STAGE_4_GOLD_DIM")

        if "AR_BAL_LCY" in df_intf.columns and "AR_BAL_LCY" in df_dim.columns:
            reconciler.audit_sum_metric(df_intf, df_dim, "AR_BAL_LCY", "AR_BAL_LCY", "STAGE_4_GOLD_DIM", "BALANCE_SUM_VND_TOTAL")
        if "AR_LMT_AMT" in df_intf.columns and "AR_LMT_AMT" in df_dim.columns:
            reconciler.audit_sum_metric(df_intf, df_dim, "AR_LMT_AMT", "AR_LMT_AMT", "STAGE_4_GOLD_DIM", "CREDIT_LIMIT_SUM_TOTAL")
    else:
        print("[WARN] Gold table DIM_LOAN_AR is missing - skipping Stage 4 checks.")

    # --- SUMMARY & REPORT SAVE ---
    df_recon_summary = reconciler.get_summary_dataframe()
    print("\n============================================================")
    print(f"[RECON TRACK 2 SUMMARY REPORT FOR ETL DATE {etl_date}]")
    print("============================================================")
    df_recon_summary.show(50, truncate=False)

    # Save Audit Summary to Iceberg Table s3a://gold/RECON_TRACK2_SUMMARY/
    write_iceberg_table(df_recon_summary, bucket="gold", schema_name=None, table_name="RECON_TRACK2_SUMMARY", mode="overwrite")
    print(f"[SUCCESS] Saved Reconciliation Audit Log to Iceberg table s3a://gold/RECON_TRACK2_SUMMARY/")

    # Check for any FAIL status
    failed_checks = df_recon_summary.filter(df_recon_summary["STATUS"] == "FAIL").count()
    if failed_checks > 0:
        print(f"[ERROR] Reconciliation Failed! Found {failed_checks} failed audit checks.")
        raise ValueError(f"Reconciliation Failed with {failed_checks} failed checks for ETL Date: {etl_date}")
    else:
        print(f"[SUCCESS] All Track 2 Reconciliation checks PASSED cleanly for ETL Date: {etl_date}")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-07"
    run_track2_reconciliation(etl_dt)
