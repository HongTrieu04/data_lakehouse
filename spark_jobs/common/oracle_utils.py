import os
import yaml
from pyspark.sql import DataFrame, SparkSession

def get_oracle_config(config_path: str = "/opt/airflow/configs/env/local.yaml"):
    """Dynamically resolves Oracle connection configuration.
    Priority 1: Environment Variables (.env)
    Priority 2: Local YAML Configuration file
    """
    yaml_cfg = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_cfg = yaml.safe_load(f).get("oracle", {})
        except Exception:
            yaml_cfg = {}

    url = os.getenv("ORACLE_JDBC_URL") or yaml_cfg.get("url") or "jdbc:oracle:thin:@//localhost:1521/ORCL"
    user = os.getenv("ORACLE_USER") or yaml_cfg.get("user") or "db_user"
    password = os.getenv("ORACLE_PASSWORD") or yaml_cfg.get("password") or "db_password"
    driver = os.getenv("ORACLE_DRIVER") or yaml_cfg.get("driver") or "oracle.jdbc.driver.OracleDriver"

    return {"url": url, "user": user, "password": password, "driver": driver}

def read_oracle_table(spark: SparkSession, schema_table: str, etl_date: str = None, filter_col: str = None) -> DataFrame:
    """Reads a table from Oracle DB using JDBC dynamically configured from .env."""
    cfg = get_oracle_config()

    query = f"(SELECT * FROM {schema_table}) t"
    if etl_date and filter_col:
        query = f"(SELECT * FROM {schema_table} WHERE TO_CHAR({filter_col}, 'YYYY-MM-DD') = '{etl_date}') t"

    df = spark.read \
        .format("jdbc") \
        .option("url", cfg["url"]) \
        .option("dbtable", query) \
        .option("user", cfg["user"]) \
        .option("password", cfg["password"]) \
        .option("driver", cfg["driver"]) \
        .load()
        
    return df
