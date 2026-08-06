-- Task 10: LOAN_AR (Loan Arrangement)
-- Layer: Silver Temp 2 (Business Logic - SCD Type 1)
-- Source: T24CORE.LD_LOANS_AND_DEPOSITS + FLEXBO + LOS_APP

CREATE OR REPLACE TABLE demo.default.loan_ar AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", ld.RECID)    AS AR_ID,
    ld.RECID                                        AS AR_CODE,
    ld.ACC_FCC                                      AS FCC_AR_CODE,
    hash("T24_CUSTOMER", ld.CUSTOMER_ID)            AS CST_ID,
    ld.CUSTOMER_ID                                  AS CST_CODE,
    hash("T24_LIMIT", ld.LIMIT_REFERENCE)           AS LMT_ID,
    ld.LIMIT_REFERENCE                              AS LMT_CODE,
    ld.CATEGORY                                     AS PD_CGY_CODE,
    ld.CURRENCY                                     AS CCY_CODE,
    ld.APPROVE_AMOUNT                               AS ORIG_AMT_FCY,
    ld.DRAWDOWN_NET_AMT                             AS DSBR_AMT_FCY,
    ld.EXCG_RATE                                    AS EXG_RATE_TO_LCL_CCY,
    ld.VALUE_DATE                                   AS EFF_DT,
    ld.FIN_MAT_DATE                                 AS MAT_DT,
    ld.ORIG_VAL_DATE                                AS VAL_DT,
    ld.LOC_TERM                                     AS TERM_CODE,
    ld.INPUTTER                                     AS MAKER_OFCR_CODE,
    ld.AUTHORISER                                   AS CHKER_OFCR_CODE,
    m.BOOKING_DATE                                  AS FCC_VAL_DT,
    'T24_LD_LOANS_AND_DEPOSITS'                     AS SRC_STM_CODE,
    CURRENT_TIMESTAMP()                             AS SYS_EFF_DT,
    TIMESTAMP('9999-12-31 23:59:59')                AS SYS_EXP_DT,
    CURRENT_TIMESTAMP()                             AS SYS_UDT_DT
FROM demo.default.bz_t24_ld_loans_and_deposits ld
LEFT JOIN demo.default.bz_flexbo_pgb_ldtb_contract_master m ON ld.ACC_FCC = m.CONTRACT_REF_NO;
