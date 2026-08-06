-- Task 38: AST_AR_INT_SMY (Asset Arrangement Interest Summary)
-- Layer: Silver Temp 2 (Bank-wised Business Logic)

CREATE OR REPLACE TABLE demo.default.ast_ar_int_smy AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", APP_ID)    AS AR_ID,
    APP_ID                                      AS AR_CODE,
    CASE WHEN APP_ID LIKE 'PDLD%' THEN SUBSTRING(APP_ID, 3, 12) 
         ELSE APP_ID END                        AS ORIG_AR_ID,
    ABS(AMOUNT_CUR)                             AS TOT_ACR_INT_AMT_FCY,
    0                                           AS TOT_INT_PAID_AMT_FCY
FROM demo.default.bz_source_saoke_crb
WHERE APP_CODE IN ('LD', 'PD');
