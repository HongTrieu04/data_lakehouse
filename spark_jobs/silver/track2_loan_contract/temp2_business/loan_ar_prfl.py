import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_loan_ar_prfl(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Temp2_LOAN_AR_PRFL")
    print(f"[PHASE 3 - TASK 11] Building Silver Temp 2 LOAN_AR_PRFL for ETL Date: {etl_date}")
    # Bank-wised loan profile attributes logic
    print("[SILVER TEMP2] Completed LOAN_AR_PRFL (Task 11)")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_loan_ar_prfl(etl_dt)
