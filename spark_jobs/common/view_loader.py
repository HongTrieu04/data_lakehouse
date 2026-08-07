import os
from spark_jobs.common.io_utils import read_parquet

BRONZE_TABLE_SCHEMAS = {
    "bz_t24core_ld_loans_and_deposits": "RECID string, AMOUNT string, CUSTOMER_ID string, CO_CODE string, PRIN_LIQ_ACCT string, INT_LIQ_ACCT string, DRAWDOWN_NET_AMT string, ORIG_VAL_DATE string, VALUE_DATE string, FIN_MAT_DATE string, INT_RATE_TYPE string, INTEREST_SPREAD string, INTEREST_RATE string, REAL_RATE string, CATEGORY string, APPROVE_AMOUNT string, DATE_TIME string, CURR_NO string, ACC_FCC string, CURRENCY string, RECORD_STATUS string, INT_VALUE_DATE string, USE_OF_LOAN string, USE_OF_LOAN_CHA string, CRA_LD string, EXCG_RATE string, MB_RM_BANCHEO string, LOC_TERM string, COMMIT_ACTION string, AUTHORISER string, INPUTTER string, LOS_FAC_CODE string, ORIGINAL_MAT string, PRODUCTGR_CODE string, FCC_LIMIT string, STATUS string, LIMIT_REFERENCE string, PE_RATE string, PS_RATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_ld_loans_and_deposits_his": "RECID string, AMOUNT string, CUSTOMER_ID string, CO_CODE string, PRIN_LIQ_ACCT string, INT_LIQ_ACCT string, DRAWDOWN_NET_AMT string, ORIG_VAL_DATE string, VALUE_DATE string, FIN_MAT_DATE string, INT_RATE_TYPE string, INTEREST_SPREAD string, INTEREST_RATE string, REAL_RATE string, CATEGORY string, APPROVE_AMOUNT string, DATE_TIME string, CURR_NO string, ACC_FCC string, CURRENCY string, RECORD_STATUS string, INT_VALUE_DATE string, USE_OF_LOAN string, USE_OF_LOAN_CHA string, CRA_LD string, EXCG_RATE string, MB_RM_BANCHEO string, LOC_TERM string, COMMIT_ACTION string, ORIGINAL_MAT string, PRODUCTGR_CODE string, STATUS string, PE_RATE string, PS_RATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_flexbo_pgb_ldtb_contract_master": "CONTRACT_REF_NO string, VERSION_NO string, BOOKING_DATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_flexbo_pgb_los_contract_fields_tdate": "CONTRACT_REF_NO string, MOV_DATE string, BIEN_DO_THAY_DOI_LS_THEO_HDTD string, SAN_PHAM_CHO_VAY_KHCN string, SAN_PHAM_CHO_VAY_KHDN string, MDVAY_SP string, SO_LAN_CO_CAU string, CAR_APPLICATION_CODE string, LOS_CONTRACT_CODE string, LOS_CONTRACT_ID string, CO_CAU_NO_MIEN_GIAM_LAI string, MDVAY_NKT string, CAN_BO_DE_XUAT_GN string, TT_PP_DE_XUAT_GN string, CBTD_QL_KHOAN_VAY string, TP_PP_QL_KHOAN_VAY string, CAP_PHE_DUYET_GNTL string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_los_app_loan_disbursement": "ID string, EXT_REF_NO_1 string, AMND_STATE string, STATUS string, FACILITY_ID string, BOOKING_DATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_los_app_facility": "ID string, PRODUCT_ID string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_los_app_product": "ID string, PRODUCT_CODE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_customer": "RECID string, KHOI string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_ebanking_col_udf_value": "COL_REF string, FUNCTION_CODE string, UDF_ID string, UDF_VALUE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_flexbo_pgb_contract_udf_map": "CONTRACT_REF_NO string, FIELD_NAME string, FIELD_VAL string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_flexbo_pgbld_contract_udfield_hist": "CONTRACT_REF_NO string, CT_KHUYEN_MAI_CHO_VAY string, CTKM2 string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_mb_mg_saving_multi": "LD_ID string, MB_LD_TYPE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_flexbo_pgbld_rt_contract_udfield_hist": "CONTRACT_REF_NO string, CO_CAU_NO string, NGAY_HET_COVID19 string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_source_saoke_mvmt": "TRANS_REF string, SBV_CODE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_source_saoke_crb": "APP_ID string, ASSET_TYPE string, SBV_CODE string, ACC_FCC string, AMOUNT_CUR string, AMOUNT_LCY string, APP_CODE string, CATEG_CODE string, CUS_ID string, VAL_DATE string, MAT_DATE string, CO_CODE string, SUB_PRO string, CURR string, LOC_TERM string, TXN_DATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_stmt_entry": "RECID string, OUR_REFERENCE string, TRANSACTION_CODE string, CRF_PROD_CAT string, CURRENCY string, AMOUNT_LCY string, AMOUNT_FCY string, BOOKING_DATE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_pd_payment_due_his_mv": "RECID string, PAY_TYPE string, FLAG_STATUS string, RECORD_STATUS string, PAY_AMT_OUTS string, DATE_TIME string, CURR_NO string, CATEGORY string, PAYMENT_DTE_DUE string, PD_TYPE string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_pd_payment_due": "RECID string, SNQH_CHUYENDOI string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_t24core_company": "RECID string, COMPANY_NAME string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_pg_t24core_currency": "ID string, MID_REVAL_RATE string, DATE_TIME string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
    "bz_pg_t24core_currency_his": "ID string, MID_REVAL_RATE string, DATE_TIME string, CURR_NO string, BRZ_LOAD_DT timestamp, SRC_SYSTEM string, ETL_BATCH_ID string",
}

BRONZE_TABLE_LOCATION_MAP = {
    "bz_t24core_ld_loans_and_deposits": ("PG_T24CORE", "LD_LOANS_AND_DEPOSITS"),
    "bz_t24core_ld_loans_and_deposits_his": ("PG_T24CORE", "LD_LOANS_AND_DEPOSITS_HIS"),
    "bz_flexbo_pgb_ldtb_contract_master": ("PG_FLEXBO", "PGB_LDTB_CONTRACT_MASTER"),
    "bz_flexbo_pgb_los_contract_fields_tdate": ("PG_FLEXBO", "PGB_LOS_CONTRACT_FIELDS_TDATE"),
    "bz_los_app_loan_disbursement": ("PG_LOS", "LOAN_DISBURSEMENT"),
    "bz_los_app_facility": ("PG_LOS", "FACILITY"),
    "bz_los_app_product": ("PG_LOS", "PRODUCT"),
    "bz_t24core_customer": ("PG_T24CORE", "CUSTOMER"),
    "bz_ebanking_col_udf_value": ("PG_EBANKING", "COL_UDF_VALUE"),
    "bz_flexbo_pgb_contract_udf_map": ("PG_FLEXBO", "PGB_CONTRACT_UDF_MAP"),
    "bz_flexbo_pgbld_contract_udfield_hist": ("PG_FLEXBO", "PGBLD_CONTRACT_UDFIELD_HIST"),
    "bz_t24core_mb_mg_saving_multi": ("PG_T24CORE", "MB_MG_SAVING_MULTI"),
    "bz_flexbo_pgbld_rt_contract_udfield_hist": ("PG_FLEXBO", "PGBLD_RT_CONTRACT_UDFIELD_HIST"),
    "bz_source_saoke_mvmt": ("PG_SAOKE", "SAOKE_MVMT"),
    "bz_source_saoke_crb": ("PG_SAOKE", "SAOKE_CRB"),
    "bz_t24core_stmt_entry": ("PG_T24CORE", "STMT_ENTRY"),
    "bz_t24core_pd_payment_due_his_mv": ("PG_T24CORE", "PD_PAYMENT_DUE_HIS_MV"),
    "bz_t24core_pd_payment_due": ("PG_T24CORE", "PD_PAYMENT_DUE"),
    "bz_t24core_company": ("PG_T24CORE", "COMPANY"),
    "bz_pg_t24core_currency": ("PG_T24CORE", "CURRENCY"),
    "bz_pg_t24core_currency_his": ("PG_T24CORE", "CURRENCY_HIS"),
}

SILVER_TABLE_SCHEMAS = {
    "AR_BAL": "AR_ID string, AR_CODE string, CST_ID string, RCVB_AMT_LCY double, CLS_BAL_LCY double, MUD_CODE string, AST_TP_CODE string, PD_CGY_CODE string, OU_CODE string, CCY_CODE string, CST_CODE string, AR_TERM_TP_CODE string, GL_ITM_CODE string, GL_ITM_ID string, SUB_PD_CODE string, CDR_DT date, SYS_UDT_DT timestamp",
    "AR_DLQ_SMY": "AR_ID string, AR_CODE string, ORIG_AR_ID string, PNP_PAST_DUE_DT date, INT_PAST_DUE_DT date, PNP_ARS double, INT_ARS double, ADDITION_DYS_IN_ARS double, SYS_UDT_DT timestamp",
    "AR_RATE_HIST": "AR_ID string, AR_CODE string, INT_RATE_VARIABLITY_TP_CODE string, INT_RATE_EFF_DT date, EFF_RATE_PCT double, SPRD_RATE_PCT double, PNY_RATE double, ODUE_INT_RATE double, MRGN_RATE double, SYS_EFF_DT date, SYS_EXP_DT date, SYS_UDT_DT timestamp",
    "AST_AR_INT_SMY": "AR_ID string, AR_CODE string, ORIG_AR_ID string, ORIG_AR_CODE string, TOT_ACR_INT_AMT_FCY double, TOT_INT_PAID_AMT_FCY double, TOT_INT_DUE_AMT_FCY double, TOT_INT_ODUE_AMT_FCY double, SYS_UDT_DT timestamp",
    "EXG_RATE": "SNPST_DT date, SRC_STM_CODE string, FRST_CCY_CODE string, SCD_CCY_CODE string, MID_RATE double, SYS_UDT_DT timestamp",
    "OU": "OU_ID string, OU_CODE string, OU_NM string, SYS_EFF_DT date, SYS_EXP_DT date, SYS_UDT_DT timestamp",
    "LOAN_AR": "AR_ID string, AR_CODE string, FCC_AR_CODE string, ORIG_AMT_FCY double, CHKER_OFCR_CODE string",
    "LOAN_AR_PRFL": "AR_ID string, AR_CODE string, DSBR_PRPSL_OFCR string, DSBR_PRPSL_MGR string",
    "INTF_LOAN_AR": "CDR_DT date, AR_CODE string, EFF_DT date, PD_CGY_CODE string, OU_CODE string, CCY_CODE string, AR_BAL_LCY double, AR_LMT_AMT double, SYS_UDT_DT timestamp",
    "DIM_LOAN_AR": "AR_CODE string, AR_ID string, AR_BAL_LCY double, AR_LMT_AMT double, SYS_UDT_DT timestamp"
}

def load_all_bronze_views(spark, etl_date: str = "2026-08-06"):
    """Loads all ingested Bronze Iceberg tables into temporary views for Spark SQL queries."""
    for trg, schema_str in BRONZE_TABLE_SCHEMAS.items():
        schema_name, raw_table_name = BRONZE_TABLE_LOCATION_MAP.get(trg, ("PG_T24CORE", trg.upper()))
        iceberg_path = f"s3a://bronze/{schema_name}/{raw_table_name}/data/"
        try:
            df = read_parquet(spark, iceberg_path)
            df.createOrReplaceTempView(trg)
        except Exception:
            df_empty = spark.createDataFrame([], schema_str)
            df_empty.createOrReplaceTempView(trg)

def load_silver_view(spark, table_name: str, etl_date: str = "2026-08-06"):
    """Loads a Silver layer output Iceberg table into temporary views for downstream SQL queries."""
    tbl_upper = table_name.upper()
    iceberg_path = f"s3a://silver/{tbl_upper}/data/"
    try:
        df = read_parquet(spark, iceberg_path)
        df.createOrReplaceTempView(tbl_upper)
        df.createOrReplaceTempView(table_name.lower())
    except Exception as e:
        print(f"[WARN] Silver view registration fallback for {table_name}: {str(e)}")
        schema_str = SILVER_TABLE_SCHEMAS.get(tbl_upper, "AR_ID string, AR_CODE string, AR_BAL_LCY double")
        df_empty = spark.createDataFrame([], schema_str)
        df_empty.createOrReplaceTempView(tbl_upper)
        df_empty.createOrReplaceTempView(table_name.lower())
