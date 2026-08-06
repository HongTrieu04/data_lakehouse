-- Task 12: DIM_LOAN_AR (Dimension Loan Arrangement)
-- Layer: Gold / Data Mart (SCD Type 2)
-- Source: SILVER.LOAN_AR + SILVER.LOAN_AR_PRFL

CREATE OR REPLACE TABLE demo.default.dim_loan_ar AS
SELECT 
    CONCAT(DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd'), '_', a.AR_ID) AS DIM_KEY,
    a.AR_ID                                     AS AR_ID,
    a.AR_CODE                                   AS AR_CODE,
    a.FCC_AR_CODE                               AS FCC_AR_CODE,
    a.PPR_CTR_NBR                               AS PPR_CTR_NBR,
    a.PD_CGY_CODE                               AS PD_CGY_CODE,
    a.AR_PPS_GRP_CODE                           AS AR_PPS_GRP_CODE,
    a.AR_PPS_TP_CODE                            AS AR_PPS_TP_CODE,
    a.ORIG_AMT_FCY                              AS ORIG_AMT_FCY,
    a.TERM_CODE                                 AS TERM_CODE,
    a.VAL_DT                                    AS VAL_DT,
    a.EFF_DT                                    AS EFF_DT,
    a.MAT_DT                                    AS MAT_DT,
    b.AR_LCS_TP_CODE                            AS AR_LCS_TP_CODE,
    CURRENT_TIMESTAMP()                         AS SYS_EFF_DT,
    TIMESTAMP('9999-12-31 23:59:59')            AS SYS_EXP_DT
FROM demo.default.loan_ar a
LEFT JOIN demo.default.loan_ar_prfl b ON a.AR_ID = b.AR_ID;
