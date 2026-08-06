from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def check_row_count_match(src_df: DataFrame, trg_df: DataFrame) -> bool:
    """Compares row count between source DataFrame and target DataFrame."""
    src_cnt = src_df.count()
    trg_cnt = trg_df.count()
    print(f"[RECON] Source Count: {src_cnt} | Target Count: {trg_cnt}")
    return src_cnt == trg_cnt

def check_null_keys(df: DataFrame, key_col: str) -> int:
    """Returns number of null/blank records in specified key column."""
    null_cnt = df.filter(F.col(key_col).isNull() | (F.col(key_col) == "")).count()
    print(f"[RECON] Null Key Count for {key_col}: {null_cnt}")
    return null_cnt

def minus_test(df1: DataFrame, df2: DataFrame, key_cols: list) -> int:
    """Performs difference check (df1 EXCEPT df2) on key columns."""
    diff = df1.select(key_cols).subtract(df2.select(key_cols))
    diff_cnt = diff.count()
    print(f"[RECON] Minus Test Difference Count: {diff_cnt}")
    return diff_cnt
