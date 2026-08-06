-- ====================================================================
-- SATELLITE 3: AR_DLQ_SMY (Arrangement Delinquency Summary - Snapshot Daily)
-- Source: bz_t24core_pd_payment_due_his_mv LEFT JOIN bz_t24core_pd_payment_due
-- Target: demo.default.ar_dlq_smy (Silver Layer)
-- ====================================================================

WITH filtered_pd AS (
    SELECT a.*, b.SNQH_CHUYENDOI
    FROM demo.default.bz_t24core_pd_payment_due_his_mv a
    LEFT JOIN demo.default.bz_t24core_pd_payment_due b ON a.RECID = b.RECID
    WHERE a.FLAG_STATUS = 'LIVE'
      AND a.PAY_TYPE IN ('PR', 'IN')
      AND COALESCE(CAST(a.PD_TYPE AS INT), 0) <> 1
      AND (a.RECORD_STATUS IS NULL OR a.RECORD_STATUS <> 'REVE')
      AND CAST(a.PAY_AMT_OUTS AS DECIMAL(18,2)) > 0
)
SELECT 
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', SUBSTRING(RECID, 1, 14))) AS AR_ID,
    SUBSTRING(RECID, 1, 14)                                             AS AR_CODE,
    sha256(concat('T24_LD_LOANS_AND_DEPOSITS', 
           CASE WHEN RECID LIKE 'PDLD%' THEN SUBSTRING(RECID, 3, 12) 
                ELSE SUBSTRING(RECID, 1, 14) END))                     AS ORIG_AR_ID,
    MIN(CASE WHEN PAY_TYPE = 'PR' AND CATEGORY <> '21069' 
             THEN TO_DATE(PAYMENT_DTE_DUE, 'yyyyMMdd') END)             AS PNP_PAST_DUE_DT,
    MIN(CASE WHEN PAY_TYPE = 'IN' OR (CATEGORY = '21069' AND PAY_TYPE = 'PR') 
             THEN TO_DATE(PAYMENT_DTE_DUE, 'yyyyMMdd') END)             AS INT_PAST_DUE_DT,
    MIN(CASE WHEN PAY_TYPE = 'PR' AND CATEGORY <> '21069' 
             THEN CAST(PAY_AMT_OUTS AS DECIMAL(18,2)) END)             AS PNP_ARS,
    MIN(CASE WHEN PAY_TYPE = 'IN' OR (CATEGORY = '21069' AND PAY_TYPE = 'PR') 
             THEN CAST(PAY_AMT_OUTS AS DECIMAL(18,2)) END)             AS INT_ARS,
    MAX(CAST(SNQH_CHUYENDOI AS INT))                                    AS ADDITION_DYS_IN_ARS,
    current_timestamp()                                                 AS SYS_UDT_DT
FROM filtered_pd
GROUP BY SUBSTRING(RECID, 1, 14), 
         CASE WHEN RECID LIKE 'PDLD%' THEN SUBSTRING(RECID, 3, 12) ELSE SUBSTRING(RECID, 1, 14) END;
