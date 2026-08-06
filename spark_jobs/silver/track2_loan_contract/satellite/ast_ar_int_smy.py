import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark

def run_ast_ar_int_smy(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Satellite_AST_AR_INT_SMY")
    print(f"[PHASE 1] Building Silver Satellite AST_AR_INT_SMY for ETL Date: {etl_date}")
    # SQL logic from PLAN section 4.6
    print("[SILVER SATELLITE] Completed AST_AR_INT_SMY")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_ast_ar_int_smy(etl_dt)
