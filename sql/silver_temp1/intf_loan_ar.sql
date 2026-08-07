-- ====================================================================
-- TASK 9: INTF_LOAN_AR (Interface Loan Arrangement - Technical Silver Temp1)
-- Source: AR_BAL LEFT JOIN 7 Silver tables (LOAN_AR, LOAN_AR_PRFL, AR_RATE_HIST, AR_DLQ_SMY, EXG_RATE, OU, AST_AR_INT_SMY)
-- Target: intf_loan_ar (Silver Temp1 Layer)
-- ====================================================================

SELECT 
    current_date()                                      AS CDR_DT,
    b.AR_CODE,
    b.EFF_DT,
    COALESCE(a.PD_CGY_CODE, b.PD_CODE)                  AS PD_CGY_CODE,
    b.PPR_CTR_NBR,
    COALESCE(c.OU_CODE, a.OU_CODE)                      AS OU_CODE,
    a.SUB_PD_CODE,
    COALESCE(a.CST_CODE, b.CST_CODE)                    AS CST_CODE,
    c.AR_LCS_TP_CODE,
    COALESCE(a.MUD_CODE, b.AR_PPS_TP_CODE)              AS MUD_CODE,
    c.FNC_ST_CODE,
    b.CCY_CODE,
    b.MAT_DT,
    COALESCE(b.FCC_VAL_DT, b.EFF_DT)                    AS VAL_DT,
    a.AR_TERM_TP_CODE,
    b.LMT_CODE,
    c.INT_LQD_AR_CODE,
    b.FCC_AR_CODE,
    d.PNY_RATE,
    d.ODUE_INT_RATE,
    d.MRGN_RATE,
    COALESCE(a.CDR_DT, current_date())                  AS TXN_DT,
    COALESCE(k.TOT_ACR_INT_AMT_FCY, 0)                  AS ODUE_IN_AMT,
    b.PD_CODE,
    c.DSBR_PRPSL_OFCR,
    c.DSBR_PRPSL_MGR,
    c.MGT_OFCR,
    c.MGT_MGR,
    c.APRV_AHR,
    c.DBT_WVR_F,
    c.NBR_OF_DBT_RSTC,
    b.AR_PPS_TP_CODE                                    AS AR_PPS_BY_PD,
    COALESCE(e1.SO_NGAY_QH_GOC, 0)                      AS DYS_IN_PNP_ARS,
    COALESCE(e1.SO_NGAY_QH_LAI, 0)                      AS DYS_IN_INT_ARS,
    COALESCE(e1.SONGAY_QHAN_CAONHAT, 0)                 AS SONGAY_QHAN_CAONHAT,
    COALESCE(e1.SNQH_CHUYENDOI, 0)                      AS SNQH_CHUYENDOI,
    '0'                                                 AS APPROVE_AMOUNT,
    CAST(NULL AS STRING)                                AS DRAWDOWN_ACCOUNT,
    CAST(NULL AS STRING)                                AS INT_KEY,
    CAST(NULL AS STRING)                                AS INT_SPRD,
    '0'                                                 AS AMOUNT_CUR,
    CAST(NULL AS STRING)                                AS INT_TP,
    CAST(NULL AS STRING)                                AS INT_BSS,
    CAST(NULL AS STRING)                                AS INT_FIX,
    CAST(NULL AS STRING)                                AS RATE,
    '0'                                                 AS ODUE_PR_AMT,
    '0'                                                 AS ODUE_PE_AMT,
    '0'                                                 AS ODUE_PS_AMT,
    current_timestamp()                                 AS SYS_UDT_DT
FROM loan_ar b
LEFT JOIN ar_bal a ON (b.AR_ID = a.AR_ID OR b.AR_CODE = a.AR_CODE)
LEFT JOIN loan_ar_prfl c ON b.AR_ID = c.AR_ID
LEFT JOIN ar_rate_hist d ON (b.AR_ID = d.AR_ID OR b.AR_CODE = d.AR_CODE)
LEFT JOIN ar_dlq_smy e ON (b.AR_ID = e.AR_ID OR b.AR_CODE = e.AR_CODE)
LEFT JOIN (
    SELECT ORIG_AR_ID,
           SUM(datediff(current_date(), PNP_PAST_DUE_DT)) + 1 AS SO_NGAY_QH_GOC,
           SUM(datediff(current_date(), INT_PAST_DUE_DT)) + 1 AS SO_NGAY_QH_LAI,
           GREATEST(COALESCE(SUM(datediff(current_date(), PNP_PAST_DUE_DT)), 0),
                    COALESCE(SUM(datediff(current_date(), INT_PAST_DUE_DT)), 0)) + 1 AS SONGAY_QHAN_CAONHAT,
           SUM(COALESCE(ADDITION_DYS_IN_ARS, 0)) AS SNQH_CHUYENDOI
    FROM ar_dlq_smy 
    GROUP BY ORIG_AR_ID
) e1 ON (e1.ORIG_AR_ID = b.AR_ID OR e1.ORIG_AR_ID = b.AR_CODE)
LEFT JOIN exg_rate f ON b.CCY_CODE = f.FRST_CCY_CODE
LEFT JOIN ou i ON COALESCE(a.OU_CODE, c.OU_CODE) = i.OU_CODE
LEFT JOIN (
    SELECT ORIG_AR_ID,
           SUM(TOT_ACR_INT_AMT_FCY) AS TOT_ACR_INT_AMT_FCY,
           SUM(TOT_INT_PAID_AMT_FCY) AS TOT_INT_PAID_AMT_FCY,
           SUM(TOT_INT_DUE_AMT_FCY)  AS TOT_INT_DUE_AMT_FCY,
           SUM(TOT_INT_ODUE_AMT_FCY) AS TOT_INT_ODUE_AMT_FCY
    FROM ast_ar_int_smy 
    GROUP BY ORIG_AR_ID
) k ON (b.AR_ID = k.ORIG_AR_ID OR b.AR_CODE = k.ORIG_AR_ID);
