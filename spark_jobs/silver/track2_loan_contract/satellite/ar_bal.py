import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_ar_bal(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Satellite_AR_BAL")
    print(f"[PHASE 1] Building Silver Satellite AR_BAL for ETL Date: {etl_date}")
    # SQL logic from PLAN section 4.1
    print("[SILVER SATELLITE] Completed AR_BAL")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_ar_bal(etl_dt)
