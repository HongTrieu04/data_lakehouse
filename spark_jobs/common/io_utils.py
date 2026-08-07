from pyspark.sql import DataFrame

def read_parquet(spark, path: str) -> DataFrame:
    """Reads Parquet or Iceberg data files from specified S3A/MinIO path."""
    return spark.read.parquet(path)

def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    """Writes DataFrame to Parquet format on specified S3A/MinIO path."""
    df.write.mode(mode).parquet(path)

def write_iceberg_table(df: DataFrame, bucket: str, schema_name: str, table_name: str, mode: str = "overwrite") -> None:
    """
    Writes DataFrame to native Iceberg format on specified S3A/MinIO path.
    Path structure:
    - Bronze: s3a://bronze/<SCHEMA_NAME>/<TABLE_NAME>/ (with data/ and metadata/)
    - Silver: s3a://silver/<TABLE_NAME>/ (with data/ and metadata/)
    - Gold:   s3a://gold/<TABLE_NAME>/ (with data/ and metadata/)
    """
    spark = df.sparkSession
    jvm = spark._jvm
    jconf = spark._jsc.hadoopConfiguration()
    jconf.set("fs.s3a.threads.keepalivetime", "60")

    if schema_name:
        base_path = f"s3a://{bucket}/{schema_name.upper()}/{table_name.upper()}"
    else:
        base_path = f"s3a://{bucket}/{table_name.upper()}"

    data_dir = f"{base_path}/data"
    tables = jvm.org.apache.iceberg.hadoop.HadoopTables(jconf)

    iceberg_schema = jvm.org.apache.iceberg.spark.SparkSchemaUtil.convert(df._jdf.schema())
    spec = jvm.org.apache.iceberg.PartitionSpec.unpartitioned()

    try:
        table = tables.load(base_path)
    except Exception:
        table = tables.create(iceberg_schema, spec, base_path)

    if mode == "overwrite":
        df.write.mode("overwrite").parquet(data_dir)
        appender = table.newOverwrite()
        is_overwrite = True
    else:
        df.write.mode("append").parquet(data_dir)
        appender = table.newFastAppend()
        is_overwrite = False

    hadoop_path = jvm.org.apache.hadoop.fs.Path(data_dir)
    fs = hadoop_path.getFileSystem(jconf)
    file_statuses = fs.listStatus(hadoop_path)

    rec_count = df.count()
    for status in file_statuses:
        p = status.getPath().toString()
        if p.endswith(".parquet") and not status.getPath().getName().startswith("_"):
            data_file = jvm.org.apache.iceberg.DataFiles.builder(table.spec()) \
                .withPath(p) \
                .withFormat(jvm.org.apache.iceberg.FileFormat.PARQUET) \
                .withFileSizeInBytes(status.getLen()) \
                .withRecordCount(rec_count) \
                .build()
            if is_overwrite:
                appender.addFile(data_file)
            else:
                appender.appendFile(data_file)

    appender.commit()

def read_iceberg_table(spark, bucket: str, schema_name: str, table_name: str) -> DataFrame:
    """Reads data from Iceberg table path s3a://<bucket>/[<schema_name>/]<table_name>/data."""
    if schema_name:
        data_dir = f"s3a://{bucket}/{schema_name.upper()}/{table_name.upper()}/data"
    else:
        data_dir = f"s3a://{bucket}/{table_name.upper()}/data"
    return spark.read.parquet(data_dir)
