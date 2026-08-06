from pyspark.sql import DataFrame

def read_parquet(spark, path: str) -> DataFrame:
    """Reads Parquet data from specified S3A/MinIO path."""
    return spark.read.parquet(path)

def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Writes DataFrame to Parquet format on specified S3A/MinIO path."""
    df.write.mode(mode).parquet(path)

def read_iceberg_table(spark, table_name: str) -> DataFrame:
    """Reads Iceberg table using standard catalog prefix."""
    full_table = f"demo.default.{table_name}"
    return spark.read.table(full_table)

def write_iceberg_table(df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
    """Writes DataFrame to Iceberg format in default namespace."""
    full_table = f"demo.default.{table_name}"
    if mode == "overwrite":
        df.writeTo(full_table).createOrReplace()
    elif mode == "append":
        df.writeTo(full_table).append()
    else:
        df.write.mode(mode).format("iceberg").saveAsTable(full_table)
