import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from spark_jobs.common.spark_session import get_spark
from spark_jobs.common.io_utils import write_parquet, write_iceberg_table
from spark_jobs.common.view_loader import load_all_bronze_views

def run_ast_ar_int_smy(etl_date: str = "2026-08-06"):
    spark = get_spark("Silver_Satellite_AST_AR_INT_SMY")
    print(f"[PHASE 1] Building Satellite AST_AR_INT_SMY for ETL Date: {etl_date}")
    
    load_all_bronze_views(spark, etl_date)
    
    sql_path = "/opt/airflow/sql/silver_temp2/ast_ar_int_smy.sql"
    if not os.path.exists(sql_path):
        sql_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../sql/silver_temp2/ast_ar_int_smy.sql"))
        
    with open(sql_path, "r", encoding="utf-8") as f:
        query = f.read()

    df_result = spark.sql(query)
    
    write_iceberg_table(df_result, bucket="silver", schema_name=None, table_name="AST_AR_INT_SMY", mode="overwrite")
    
    print(f"[SILVER SATELLITE] Completed AST_AR_INT_SMY - Ingested successfully")

if __name__ == "__main__":
    etl_dt = sys.argv[1] if len(sys.argv) > 1 else "2026-08-06"
    run_ast_ar_int_smy(etl_dt)
