-- ====================================================================
-- SATELLITE 6: AST_AR_INT_SMY (Asset Arrangement Interest Summary)
-- Source: bz_source_saoke_crb LEFT JOIN bz_t24core_stmt_entry
-- Target: demo.default.ast_ar_int_smy (Silver Layer)
-- ====================================================================

WITH stmt_repaid AS (
    SELECT 
        CASE WHEN OUR_REFERENCE LIKE 'LD%' OR OUR_REFERENCE LIKE 'PD%' 
             THEN OUR_REFERENCE END AS RECID,
        SUM(CASE 
            WHEN OUR_REFERENCE LIKE 'PDPD%' AND TRANSACTION_CODE IN ('751', '434')
                 THEN CASE WHEN CURRENCY = 'VND' THEN ABS(CAST(AMOUNT_LCY AS DECIMAL(18,2))) ELSE ABS(CAST(AMOUNT_FCY AS DECIMAL(18,2))) END
            WHEN (OUR_REFERENCE LIKE 'PDLD%' OR OUR_REFERENCE LIKE 'LD%') AND CRF_PROD_CAT = '21069' AND TRANSACTION_CODE IN ('750', '420')
                 THEN CASE WHEN CURRENCY = 'VND' THEN ABS(CAST(AMOUNT_LCY AS DECIMAL(18,2))) ELSE ABS(CAST(AMOUNT_FCY AS DECIMAL(18,2))) END
            ELSE 0 END) AS SUM_REPAID_AMT
    FROM demo.default.bz_t24core_stmt_entry
    WHERE RECID NOT LIKE 'F%'
    GROUP BY CASE WHEN OUR_REFERENCE LIKE 'LD%' OR OUR_REFERENCE LIKE 'PD%' THEN OUR_REFERENCE END
)
SELECT 
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', a.APP_ID))  AS AR_ID,
    a.APP_ID                                              AS AR_CODE,
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', 
           CASE WHEN a.APP_ID LIKE 'PDLD%' THEN SUBSTRING(a.APP_ID, 3, 12) 
                ELSE a.APP_ID END))                      AS ORIG_AR_ID,
    CASE WHEN a.APP_ID LIKE 'PDLD%' THEN SUBSTRING(a.APP_ID, 3, 12) 
         ELSE a.APP_ID END                                AS ORIG_AR_CODE,
    ABS(CAST(a.AMOUNT_CUR AS DECIMAL(18,2)))              AS TOT_ACR_INT_AMT_FCY,
    COALESCE(b.SUM_REPAID_AMT, 0)                         AS TOT_INT_PAID_AMT_FCY,
    CASE WHEN a.SBV_CODE LIKE '394%' 
         THEN ABS(COALESCE(CAST(a.AMOUNT_CUR AS DECIMAL(18,2)), 0)) 
         ELSE 0 END                                       AS TOT_INT_DUE_AMT_FCY,
    CASE WHEN a.SBV_CODE LIKE '94%' 
         THEN ABS(COALESCE(CAST(a.AMOUNT_CUR AS DECIMAL(18,2)), 0)) 
         ELSE 0 END                                       AS TOT_INT_ODUE_AMT_FCY,
    current_timestamp()                                   AS SYS_UDT_DT
FROM demo.default.bz_source_saoke_crb a
LEFT JOIN stmt_repaid b ON a.APP_ID = b.RECID
WHERE a.APP_CODE IN ('LD', 'PD');
