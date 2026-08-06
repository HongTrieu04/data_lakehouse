-- Task 11: LOAN_AR_PRFL (Loan Arrangement Profile)
-- Layer: Silver Temp 2 (Bank-wised Profile - SCD Type 2)
-- Source: T24CORE.LD_LOANS_AND_DEPOSITS + EBANKING + FLEXBO + LOS_APP

CREATE OR REPLACE TABLE demo.default.loan_ar_prfl AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", a.RECID)      AS AR_ID,
    a.INT_LIQ_ACCT                                  AS INT_LQD_AR_CODE,
    a.PRIN_LIQ_ACCT                                 AS PNP_LQD_AR_CODE,
    a.STATUS                                        AS AR_LCS_TP_CODE,
    a.CO_CODE                                       AS OU_CODE,
    b.UDF_VALUE                                     AS MAND_REPYMT,
    COALESCE(d.SO_LAN_CO_CAU, c.FIELD_VAL)          AS NBR_OF_DBT_RSTC,
    c1.FIELD_VAL                                    AS SCR_TP,
    d.CAR_APPLICATION_CODE                          AS CAR_AP_CODE,
    d.LOS_CONTRACT_CODE                             AS LOS_CTR_CODE,
    d.LOS_CONTRACT_ID                               AS LOS_CTR_ID,
    d.CO_CAU_NO_MIEN_GIAM_LAI                       AS DBT_WVR_F,
    d.CAN_BO_DE_XUAT_GN                             AS DSBR_PRPSL_OFCR,
    d.TT_PP_DE_XUAT_GN                              AS DSBR_PRPSL_MGR,
    d.CBTD_QL_KHOAN_VAY                             AS MGT_OFCR,
    d.TP_PP_QL_KHOAN_VAY                            AS MGT_MGR,
    d.CAP_PHE_DUYET_GNTL                            AS APRV_AHR,
    a.AUTHORISER                                    AS CHKER_OFCR,
    a.INPUTTER                                      AS MAKER_OFCR,
    '1'                                             AS FNC_ST_CODE,
    CURRENT_TIMESTAMP()                             AS SYS_EFF_DT,
    TIMESTAMP('9999-12-31 23:59:59')                AS SYS_EXP_DT,
    CURRENT_TIMESTAMP()                             AS SYS_UDT_DT
FROM demo.default.bz_t24_ld_loans_and_deposits a
LEFT JOIN demo.default.bz_ebanking_col_udf_value b ON b.COL_REF = a.ACC_FCC AND b.UDF_ID = 'NHANNO_BATBUOC'
LEFT JOIN demo.default.bz_flexbo_pgb_contract_udf_map c ON c.CONTRACT_REF_NO = a.ACC_FCC AND c.FIELD_NAME = 'SO LAN CO CAU'
LEFT JOIN demo.default.bz_flexbo_pgb_contract_udf_map c1 ON c1.CONTRACT_REF_NO = a.ACC_FCC AND c1.FIELD_NAME = 'LOAI CHUNG KHOAN'
LEFT JOIN demo.default.bz_flexbo_pgb_los_contract_fields_tdate d ON d.CONTRACT_REF_NO = a.RECID;
