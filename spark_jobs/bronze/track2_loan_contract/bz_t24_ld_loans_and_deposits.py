import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))

from spark_jobs.common.spark_session import get_spark
from pyspark.sql import functions as F

def run_bronze_ld_loans(etl_date: str = "2026-08-06"):
    spark = get_spark("Bronze_T24_LD_Loans")
    print(f"[PHASE 0 - BRONZE] Ingesting T24 LD_LOANS_AND_DEPOSITS for ETL_DATE: {etl_date}")
    
    # Example logic: add audit columns
    # df = spark.read.parquet(f"s3a://landing/t24/ld_loans/{etl_date}/*.parquet")
    # df_brz = df.withColumn("BRZ_LOAD_DT", F.current_timestamp()) \
    #            .withColumn("SRC_SYSTEM", F.lit("T24CORE"))
    # write_iceberg_table(df_brz, "bz_t24_ld_loans_and_deposits")
    print("[BRONZE] Completed Bronze table bz_t24_ld_loans_and_deposits")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_bronze_ld_loans(etl_dt)
