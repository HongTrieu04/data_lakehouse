-- Task 9: INTF_LOAN_AR (Interface Loan Arrangement)
-- Layer: Silver Temp 1 (Technical Join)
-- Source: Join 8 Silver tables (AR_BAL, LOAN_AR, LOAN_AR_PRFL, AR_RATE_HIST, AR_DLQ_SMY, EXG_RATE, OU, AST_AR_INT_SMY)

CREATE OR REPLACE TABLE demo.default.intf_loan_ar AS
SELECT 
    CURRENT_DATE()                          AS CDR_DT,
    a.AR_CODE                               AS AR_CODE,
    b.EFF_DT                                AS EFF_DT,
    a.PD_CGY_CODE                           AS PD_CGY_CODE,
    b.PPR_CTR_NBR                           AS PPR_CTR_NBR,
    c.OU_CODE                               AS OU_CODE,
    a.SUB_PD_CODE                           AS SUB_PD_CODE,
    a.CST_CODE                               AS CST_CODE,
    c.AR_LCS_TP_CODE                        AS AR_LCS_TP_CODE,
    a.MUD_CODE                              AS MUD_CODE,
    c.FNC_ST_CODE                           AS FNC_ST_CODE,
    b.CCY_CODE                              AS CCY_CODE,
    b.MAT_DT                                AS MAT_DT,
    COALESCE(b.FCC_VAL_DT, b.EFF_DT)        AS VAL_DT,
    a.AR_TERM_TP_CODE                       AS AR_TERM_TP_CODE,
    b.LMT_CODE                              AS LMT_CODE,
    c.INT_LQD_AR_CODE                       AS INT_LQD_AR_CODE,
    b.FCC_AR_CODE                           AS FCC_AR_CODE,
    d.PNY_RATE                              AS PNY_RATE,
    d.ODUE_INT_RATE                         AS ODUE_INT_RATE,
    d.MRGN_RATE                             AS MRGN_RATE,
    a.CDR_DT                                AS TXN_DT,
    k.TOT_ACR_INT_AMT_FCY                   AS ODUE_IN_AMT,
    b.PD_CODE                               AS PD_CODE,
    c.DSBR_PRPSL_OFCR                       AS DSBR_PRPSL_OFCR,
    c.DSBR_PRPSL_MGR                        AS DSBR_PRPSL_MGR,
    c.MGT_OFCR                              AS MGT_OFCR,
    c.MGT_MGR                               AS MGT_MGR,
    c.APRV_AHR                              AS APRV_AHR,
    c.DBT_WVR_F                             AS DBT_WVR_F,
    c.NBR_OF_DBT_RSTC                       AS NBR_OF_DBT_RSTC,
    b.AR_PPS_TP_CODE                        AS AR_PPS_BY_PD,
    e1.SO_NGAY_QH_GOC                       AS DYS_IN_PNP_ARS,
    e1.SO_NGAY_QH_LAI                       AS DYS_IN_INT_ARS
FROM demo.default.ar_bal a
JOIN demo.default.loan_ar b ON a.AR_ID = b.AR_ID
JOIN demo.default.loan_ar_prfl c ON a.AR_ID = c.AR_ID
JOIN demo.default.ar_rate_hist d ON a.AR_ID = d.AR_ID
LEFT JOIN demo.default.ar_dlq_smy e ON a.AR_ID = e.AR_ID
LEFT JOIN (
    SELECT ORIG_AR_ID,
           SUM(DATEDIFF(CURRENT_DATE(), PNP_PAST_DUE_DT)) + 1 AS SO_NGAY_QH_GOC,
           SUM(DATEDIFF(CURRENT_DATE(), INT_PAST_DUE_DT)) + 1 AS SO_NGAY_QH_LAI
    FROM demo.default.ar_dlq_smy 
    GROUP BY ORIG_AR_ID
) e1 ON e1.ORIG_AR_ID = a.AR_ID
LEFT JOIN demo.default.exg_rate f ON b.CCY_CODE = f.FRST_CCY_CODE
LEFT JOIN demo.default.ou i ON a.OU_CODE = i.OU_CODE
LEFT JOIN (
    SELECT ORIG_AR_ID,
           SUM(TOT_ACR_INT_AMT_FCY) AS TOT_ACR_INT_AMT_FCY
    FROM demo.default.ast_ar_int_smy 
    GROUP BY ORIG_AR_ID
) k ON a.AR_ID = k.ORIG_AR_ID;
