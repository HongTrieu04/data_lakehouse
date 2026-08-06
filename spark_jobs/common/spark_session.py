import os
import yaml
from pyspark.sql import SparkSession

class SparkSessionManager:
    _instance = None

    @classmethod
    def get_spark_session(cls, app_name="DataLakehouse_Job", config_path="/opt/airflow/configs/env/local.yaml"):
        if cls._instance is None:
            # Load YAML configuration if exists
            cfg = {}
            if os.path.exists(config_path):
                with open(config_path, "r", encoding="utf-8") as f:
                    cfg = yaml.safe_load(f)

            minio_endpoint = cfg.get("storage", {}).get("minio", {}).get("endpoint", "http://minio:9000")
            access_key = cfg.get("storage", {}).get("minio", {}).get("access_key", "admin")
            secret_key = cfg.get("storage", {}).get("minio", {}).get("secret_key", "password123")
            hive_uri = cfg.get("catalog", {}).get("uri", "thrift://hive-metastore:9083")
            warehouse = cfg.get("catalog", {}).get("warehouse", "s3a://gold/iceberg/")

            builder = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
                .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
                .config("spark.sql.catalog.demo.type", "hive") \
                .config("spark.sql.catalog.demo.uri", hive_uri) \
                .config("spark.sql.catalog.demo.warehouse", warehouse) \
                .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
                .config("spark.hadoop.fs.s3a.access.key", access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
                .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

            cls._instance = builder.getOrCreate()
        return cls._instance

def get_spark(app_name="DataLakehouse_Job"):
    return SparkSessionManager.get_spark_session(app_name=app_name)
