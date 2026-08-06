-- ====================================================================
-- SATELLITE 2: AR_RATE_HIST (Arrangement Rate History - SCD Type 2)
-- Source: bz_t24core_ld_loans_and_deposits LEFT JOIN bz_flexbo_pgb_los_contract_fields_tdate
-- Target: demo.default.ar_rate_hist (Silver Layer)
-- ====================================================================

SELECT 
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', a.RECID)) AS AR_ID,
    a.RECID                                             AS AR_CODE,
    a.INT_RATE_TYPE                                     AS INT_RATE_VARIABLITY_TP_CODE,
    TO_DATE(a.INT_VALUE_DATE, 'yyyyMMdd')              AS INT_RATE_EFF_DT,
    CAST(a.INTEREST_RATE AS DECIMAL(10,4))              AS EFF_RATE_PCT,
    CAST(a.INTEREST_SPREAD AS DECIMAL(10,4))            AS SPRD_RATE_PCT,
    CAST(a.PE_RATE AS DECIMAL(10,4))                    AS PNY_RATE,
    CAST(a.PS_RATE AS DECIMAL(10,4))                    AS ODUE_INT_RATE,
    CAST(d.BIEN_DO_THAY_DOI_LS_THEO_HDTD AS DECIMAL(10,4)) AS MRGN_RATE,
    current_date()                                      AS SYS_EFF_DT,
    TO_DATE('9999-12-31', 'yyyy-MM-dd')                AS SYS_EXP_DT,
    current_timestamp()                                 AS SYS_UDT_DT
FROM demo.default.bz_t24core_ld_loans_and_deposits a
LEFT JOIN demo.default.bz_flexbo_pgb_los_contract_fields_tdate d
  ON d.CONTRACT_REF_NO = a.RECID 
 AND d.MOV_DATE = COALESCE(a.ORIG_VAL_DATE, a.VALUE_DATE);
