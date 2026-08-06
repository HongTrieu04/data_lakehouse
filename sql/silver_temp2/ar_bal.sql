-- ====================================================================
-- SATELLITE 1: AR_BAL (Arrangement Balance - Snapshot Daily)
-- Source: bz_source_saoke_crb
-- Target: demo.default.ar_bal (Silver Layer)
-- ====================================================================

SELECT 
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', APP_ID)) AS AR_ID,
    sha256(concat('T24_CUSTOMER', CUSTOMER_ID))         AS CST_ID,
    CAST(AMOUNT_CUR AS DECIMAL(18,2))                   AS RCVB_AMT_LCY,
    CAST(AMOUNT_LCY AS DECIMAL(18,2))                   AS CLS_BAL_LCY,
    APP_CODE                                            AS MUD_CODE,
    APP_ID                                              AS AR_CODE,
    ASSET_TYPE                                          AS AST_TP_CODE,
    CATEG_CODE                                          AS PD_CGY_CODE,
    CO_CODE                                             AS OU_CODE,
    CURR                                                AS CCY_CODE,
    CUS_ID                                              AS CST_CODE,
    LOC_TERM                                            AS AR_TERM_TP_CODE,
    SBV_CODE                                            AS GL_ITM_CODE,
    SBV_CODE                                            AS GL_ITM_ID,
    SUB_PRO                                             AS SUB_PD_CODE,
    TO_DATE(TXN_DATE, 'yyyyMMdd')                       AS CDR_DT,
    current_timestamp()                                 AS SYS_UDT_DT
FROM demo.default.bz_source_saoke_crb
WHERE APP_CODE IN ('LD', 'PD');
