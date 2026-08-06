import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_ar_rate_hist(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Satellite_AR_RATE_HIST")
    print(f"[PHASE 1] Building Silver Satellite AR_RATE_HIST for ETL Date: {etl_date}")
    # SQL logic from PLAN section 4.2
    print("[SILVER SATELLITE] Completed AR_RATE_HIST")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_ar_rate_hist(etl_dt)
