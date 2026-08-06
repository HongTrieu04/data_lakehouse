import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from spark_jobs.common.spark_session import get_spark

def run_landing_extract(etl_date: str = "2026-08-06"):
    spark = get_spark("Landing_Track2_Loan_Sources")
    print(f"[PHASE 0] Starting Landing Extraction for ETL Date: {etl_date}")
    
    # 21 Source tables list for Track 2
    sources = [
        "LD_LOANS_AND_DEPOSITS", "LD_LOANS_AND_DEPOSITS_HIS", "PGB_LDTB_CONTRACT_MASTER",
        "PGB_LOS_CONTRACT_FIELDS_TDATE", "LOAN_DISBURSEMENT", "FACILITY", "PRODUCT",
        "CUSTOMER", "COL_UDF_VALUE", "PGB_CONTRACT_UDF_MAP", "PGBLD_CONTRACT_UDFIELD_HIST",
        "MB_MG_SAVING_MULTI", "PGBLD_RT_CONTRACT_UDFIELD_HIST", "SAOKE_MVMT", "SAOKE_CRB",
        "STMT_ENTRY", "PD_PAYMENT_DUE_HIS_MV", "PD_PAYMENT_DUE", "COMPANY", "CURRENCY", "CURRENCY_HIS"
    ]
    
    for src in sources:
        print(f"[LANDING] Simulated Landing for table: {src}")
    
    print("[PHASE 0] Landing extraction completed successfully.")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_landing_extract(etl_dt)
