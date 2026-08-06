import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.io_utils import write_parquet, write_iceberg_table

def run_ou(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Satellite_OU")
    print(f"[PHASE 1] Building Satellite OU for ETL Date: {etl_date}")
    
    sql_path = "/opt/airflow/sql/silver_temp2/ou.sql"
    if not os.path.exists(sql_path):
        sql_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../sql/silver_temp2/ou.sql"))
        
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    df_result = spark.sql(query)
    
    target_path = f"s3a://silver/ou/{etl_date}/"
    write_parquet(df_result, target_path, mode="overwrite")
    write_iceberg_table(df_result, "ou", mode="overwrite")
    df_result.createOrReplaceTempView("ou")
    
    print(f"[SILVER SATELLITE] Completed OU - {df_result.count()} records written")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_ou(etl_dt)
