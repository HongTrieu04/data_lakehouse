-- Task 34: AR_BAL (Arrangement Balance)
-- Layer: Silver Temp 2 (Bank-wised Business Logic)

CREATE OR REPLACE TABLE demo.default.ar_bal AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", APP_ID)    AS AR_ID,
    APP_ID                                      AS AR_CODE,
    hash("T24_CUSTOMER", CUSTOMER_ID)           AS CST_ID,
    CUSTOMER_ID                                 AS CST_CODE,
    AMOUNT_CUR                                  AS RCVB_AMT_LCY,
    AMOUNT_LCY                                  AS CLS_BAL_LCY,
    APP_CODE                                    AS MUD_CODE,
    ASSET_TYPE                                  AS AST_TP_CODE,
    CATEG_CODE                                  AS PD_CGY_CODE,
    CO_CODE                                     AS OU_CODE,
    CURR                                        AS CCY_CODE,
    LOC_TERM                                    AS AR_TERM_TP_CODE,
    SUB_PRO                                     AS SUB_PD_CODE,
    TXN_DATE                                    AS CDR_DT
FROM demo.default.bz_source_saoke_crb
WHERE APP_CODE IN ('LD', 'PD');
