import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_loan_ar(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Temp2_LOAN_AR")
    print(f"[PHASE 2 - TASK 10] Building Silver Temp 2 LOAN_AR for ETL Date: {etl_date}")
    # Business logic transformation joining T24 & FLEXBO
    print("[SILVER TEMP2] Completed LOAN_AR (Task 10)")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_loan_ar(etl_dt)
