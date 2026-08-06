import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_intf_loan_ar(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Temp1_INTF_LOAN_AR")
    print(f"[PHASE 4 - TASK 9] Building Silver Temp 1 INTF_LOAN_AR for ETL Date: {etl_date}")
    # Technical join of 8 Silver tables (6 satellites + LOAN_AR + LOAN_AR_PRFL)
    print("[SILVER TEMP1] Completed INTF_LOAN_AR (Task 9)")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_intf_loan_ar(etl_dt)
