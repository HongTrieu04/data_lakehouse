import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

class Track2Reconciler:
    """
    Automated Data Reconciliation Engine for Track 2 (Loan Contract Data Lakehouse).
    Audits Data Completeness, Financial Control Totals, Key Integrity, and Discrepancies.
    """
    def __init__(self, spark: SparkSession, etl_date: str):
        self.spark = spark
        self.etl_date = etl_date
        self.audit_records = []

    def audit_row_count(self, src_df: DataFrame, trg_df: DataFrame, stage_name: str, check_name: str):
        """Audits row count equality between source and target DataFrames."""
        src_cnt = float(src_df.count()) if src_df is not None else 0.0
        trg_cnt = float(trg_df.count()) if trg_df is not None else 0.0
        variance = abs(src_cnt - trg_cnt)
        status = "PASS" if variance == 0 else "FAIL"

        print(f"[RECON | {stage_name}] {check_name} -> Src Count: {src_cnt:.0f} | Trg Count: {trg_cnt:.0f} | Variance: {variance:.0f} [{status}]")
        self.audit_records.append((
            self.etl_date, stage_name, check_name, src_cnt, trg_cnt, variance, status, datetime.datetime.now()
        ))

    def audit_null_keys(self, df: DataFrame, key_col: str, stage_name: str):
        """Audits primary key columns for null or empty string values."""
        check_name = f"NULL_KEY_CHECK_{key_col.upper()}"
        if df is None:
            null_cnt = 0.0
            status = "FAIL"
        else:
            null_cnt = float(df.filter(F.col(key_col).isNull() | (F.trim(F.col(key_col)) == "")).count())
            status = "PASS" if null_cnt == 0 else "FAIL"

        print(f"[RECON | {stage_name}] {check_name} -> Null Key Count: {null_cnt:.0f} [{status}]")
        self.audit_records.append((
            self.etl_date, stage_name, check_name, 0.0, null_cnt, null_cnt, status, datetime.datetime.now()
        ))

    def audit_sum_metric(self, src_df: DataFrame, trg_df: DataFrame, src_col: str, trg_col: str, stage_name: str, check_name: str):
        """Audits control sum totals for numeric financial metrics (e.g. balance, limit, interest)."""
        src_sum = float(src_df.select(F.coalesce(F.sum(F.col(src_col).cast("double")), F.lit(0.0))).collect()[0][0]) if src_df else 0.0
        trg_sum = float(trg_df.select(F.coalesce(F.sum(F.col(trg_col).cast("double")), F.lit(0.0))).collect()[0][0]) if trg_df else 0.0
        variance = abs(src_sum - trg_sum)
        status = "PASS" if variance < 0.01 else "WARN" if variance < 10.0 else "FAIL"

        print(f"[RECON | {stage_name}] {check_name} -> Src Sum: {src_sum:,.2f} | Trg Sum: {trg_sum:,.2f} | Variance: {variance:,.2f} [{status}]")
        self.audit_records.append((
            self.etl_date, stage_name, check_name, src_sum, trg_sum, variance, status, datetime.datetime.now()
        ))

    def audit_minus_test(self, src_df: DataFrame, trg_df: DataFrame, src_key: str, trg_key: str, stage_name: str):
        """Performs difference test (SRC EXCEPT TRG) to detect missing keys."""
        check_name = f"MINUS_TEST_{src_key.upper()}_EXCEPT_{trg_key.upper()}"
        if src_df is None or trg_df is None:
            diff_cnt = 0.0
            status = "FAIL"
        else:
            src_keys = src_df.select(F.col(src_key).alias("KEY_ID")).distinct()
            trg_keys = trg_df.select(F.col(trg_key).alias("KEY_ID")).distinct()
            diff_cnt = float(src_keys.subtract(trg_keys).count())
            status = "PASS" if diff_cnt == 0 else "FAIL"

        print(f"[RECON | {stage_name}] {check_name} -> Discrepancy Count: {diff_cnt:.0f} [{status}]")
        self.audit_records.append((
            self.etl_date, stage_name, check_name, 0.0, diff_cnt, diff_cnt, status, datetime.datetime.now()
        ))

    def get_summary_dataframe(self) -> DataFrame:
        """Returns collected audit records as PySpark DataFrame."""
        schema = StructType([
            StructField("ETL_DATE", StringType(), True),
            StructField("CHECK_STAGE", StringType(), True),
            StructField("CHECK_NAME", StringType(), True),
            StructField("SRC_METRIC_VAL", DoubleType(), True),
            StructField("TRG_METRIC_VAL", DoubleType(), True),
            StructField("VARIANCE", DoubleType(), True),
            StructField("STATUS", StringType(), True),
            StructField("CHECK_TIME", TimestampType(), True)
        ])
        if not self.audit_records:
            return self.spark.createDataFrame([], schema)
        return self.spark.createDataFrame(self.audit_records, schema)
