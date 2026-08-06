import os
import yaml
from pyspark.sql import DataFrame, SparkSession

def load_dotenv():
    """Loads .env file into os.environ if present."""
    paths = [
        "/opt/airflow/.env",
        "/opt/spark/.env",
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env")),
        os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.env")),
    ]
    for p in paths:
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k = k.strip()
                        v = v.strip().strip("'\"")
                        if k:
                            os.environ[k] = v
            break

def get_oracle_config(config_path: str = "/opt/airflow/configs/env/local.yaml"):
    """Dynamically resolves Oracle connection configuration."""
    load_dotenv()
    
    yaml_cfg = {}
    if os.path.exists(config_path):
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                yaml_cfg = yaml.safe_load(f).get("oracle", {})
        except Exception:
            yaml_cfg = {}

    host = os.getenv("ORACLE_HOST") or "localhost"
    port = os.getenv("ORACLE_PORT") or "1521"
    service_name = os.getenv("ORACLE_SERVICE_NAME") or "ORCL"
    
    # If running inside Docker container and host is localhost, resolve to host.docker.internal
    if os.path.exists("/.dockerenv") and host in ("localhost", "127.0.0.1"):
        host = "host.docker.internal"

    default_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    url = os.getenv("ORACLE_JDBC_URL") or yaml_cfg.get("url") or default_url
    if url.startswith("${"):  # Handle unexpanded variable placeholders
        url = default_url

    user = os.getenv("ORACLE_USER") or yaml_cfg.get("user") or "db_user"
    if user.startswith("${"):
        user = "db_user"

    password = os.getenv("ORACLE_PASSWORD") or yaml_cfg.get("password") or "db_password"
    if password.startswith("${"):
        password = "db_password"

    driver = os.getenv("ORACLE_DRIVER") or yaml_cfg.get("driver") or "oracle.jdbc.driver.OracleDriver"
    if driver.startswith("${"):
        driver = "oracle.jdbc.driver.OracleDriver"

    return {"url": url, "user": user, "password": password, "driver": driver, "host": host, "port": port}

def read_oracle_table(spark: SparkSession, schema_table: str, etl_date: str = None, filter_col: str = None) -> DataFrame:
    """Reads a table from Oracle DB using JDBC dynamically configured from .env."""
    cfg = get_oracle_config()

    query = f"(SELECT * FROM {schema_table}) t"
    if etl_date and filter_col:
        clean_date_no_dash = etl_date.replace("-", "")
        query = f"(SELECT * FROM {schema_table} WHERE TO_CHAR({filter_col}) LIKE '{etl_date}%' OR TO_CHAR({filter_col}) LIKE '{clean_date_no_dash}%') t"

    df = spark.read \
        .format("jdbc") \
        .option("url", cfg["url"]) \
        .option("dbtable", query) \
        .option("user", cfg["user"]) \
        .option("password", cfg["password"]) \
        .option("driver", cfg["driver"]) \
        .load()
        
    return df
