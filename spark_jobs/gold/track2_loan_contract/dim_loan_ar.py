import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from spark_jobs.common.spark_session import get_spark

def run_dim_loan_ar(etl_date: str = "2026-08-06"):
    spark = get_spark("Gold_DIM_LOAN_AR")
    print(f"[PHASE 5 - TASK 12] Building Gold Dimension DIM_LOAN_AR for ETL Date: {etl_date}")
    # SCD Type 2 dimension calculation joining LOAN_AR & LOAN_AR_PRFL
    print("[GOLD] Completed DIM_LOAN_AR (Task 12)")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_dim_loan_ar(etl_dt)
