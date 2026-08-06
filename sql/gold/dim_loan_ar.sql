-- ====================================================================
-- TASK 12: DIM_LOAN_AR (Loan Arrangement Dimension Table - Gold / Data Mart Layer SCD2)
-- Source: LOAN_AR LEFT JOIN LOAN_AR_PRFL
-- Target: dim_loan_ar (Gold Layer)
-- ====================================================================

SELECT 
    concat(date_format(current_date(), 'yyyyMMdd'), a.AR_ID) AS DIM_KEY,
    a.AR_ID,
    a.AR_CODE,
    a.FCC_AR_CODE,
    a.PPR_CTR_NBR,
    a.PD_CGY_CODE,
    a.AR_PPS_GRP_CODE,
    a.AR_PPS_TP_CODE,
    a.ORIG_AMT_FCY,
    a.TERM_CODE,
    a.VAL_DT,
    a.EFF_DT,
    a.MAT_DT,
    b.AR_LCS_TP_CODE,
    b.PROM_PRGM                                         AS PROM_CODE,
    b.PROM_PRGM_2                                       AS PROM_CODE_2,
    current_date()                                      AS SYS_EFF_DT,
    TO_DATE('9999-12-31', 'yyyy-MM-dd')                 AS SYS_EXP_DT,
    current_timestamp()                                  AS SYS_UDT_DT
FROM loan_ar a
LEFT JOIN loan_ar_prfl b ON a.AR_ID = b.AR_ID;
