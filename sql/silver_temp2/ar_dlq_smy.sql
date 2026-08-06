-- Task 36: AR_DLQ_SMY (Arrangement Delinquency Summary)
-- Layer: Silver Temp 2 (Bank-wised Business Logic)

CREATE OR REPLACE TABLE demo.default.ar_dlq_smy AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", SUBSTRING(a.RECID, 1, 14))   AS AR_ID,
    SUBSTRING(a.RECID, 1, 14)                                       AS AR_CODE,
    hash("T24_LD_LOANS_AND_DEPOSITS", 
         CASE WHEN a.RECID LIKE 'PDLD%' THEN SUBSTRING(a.RECID, 3, 12) 
              ELSE SUBSTRING(a.RECID, 1, 14) END)                  AS ORIG_AR_ID,
    MIN(CASE WHEN a.PAY_TYPE = 'PR' AND a.CATEGORY <> 21069 
             THEN TO_DATE(a.PAYMENT_DTE_DUE, 'yyyyMMdd') END)       AS PNP_PAST_DUE_DT,
    MIN(CASE WHEN a.PAY_TYPE = 'IN' OR (a.CATEGORY = 21069 AND a.PAY_TYPE = 'PR') 
             THEN TO_DATE(a.PAYMENT_DTE_DUE, 'yyyyMMdd') END)       AS INT_PAST_DUE_DT,
    MAX(b.SNQH_CHUYENDOI)                                          AS ADDITION_DYS_IN_ARS
FROM demo.default.bz_t24_pd_payment_due_his_mv a
LEFT JOIN demo.default.bz_t24_pd_payment_due b ON a.RECID = b.RECID
WHERE a.flag_status = 'LIVE'
  AND a.PAY_TYPE IN ('PR', 'IN')
  AND a.PAY_AMT_OUTS > 0
GROUP BY SUBSTRING(a.RECID, 1, 14), a.RECID;
