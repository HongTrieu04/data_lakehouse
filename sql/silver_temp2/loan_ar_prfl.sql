-- ====================================================================
-- TASK 11: LOAN_AR_PRFL (Loan Arrangement Profile Silver - SCD Type 2)
-- Source: bz_t24core_ld_loans_and_deposits (Live) UNION ALL bz_t24core_ld_loans_and_deposits_his (History)
-- Target: demo.default.loan_ar_prfl (Silver Layer)
-- ====================================================================

WITH live_prfl AS (
    SELECT 
        sha256(concat('T24_LD_LOANS_AND_DEPOSITS', a.RECID)) AS AR_ID,
        a.INT_LIQ_ACCT                                      AS INT_LQD_AR_CODE,
        sha256(concat('T24_ACCOUNT', a.INT_LIQ_ACCT))        AS INT_LQD_AR_ID,
        a.PRIN_LIQ_ACCT                                     AS PNP_LQD_AR_CODE,
        sha256(concat('T24_ACCOUNT', a.PRIN_LIQ_ACCT))       AS PNP_LQD_AR_ID,
        a.STATUS                                            AS AR_LCS_TP_CODE,
        a.CO_CODE                                           AS OU_CODE,
        b.UDF_VALUE                                         AS MAND_REPYMT,
        COALESCE(d.SO_LAN_CO_CAU, c.FIELD_VAL)              AS NBR_OF_DBT_RSTC,
        c1.FIELD_VAL                                        AS SCR_TP,
        d.CAR_APPLICATION_CODE                              AS CAR_AP_CODE,
        d.LOS_CONTRACT_CODE                                 AS LOS_CTR_CODE,
        d.LOS_CONTRACT_ID                                   AS LOS_CTR_ID,
        d.CO_CAU_NO_MIEN_GIAM_LAI                           AS DBT_WVR_F,
        d.MDVAY_NKT                                         AS AR_PPS_BY_IDY,
        d.MDVAY_SP                                          AS AR_PPS_BY_PD,
        d.CAN_BO_DE_XUAT_GN                                 AS DSBR_PRPSL_OFCR,
        d.TT_PP_DE_XUAT_GN                                  AS DSBR_PRPSL_MGR,
        d.CBTD_QL_KHOAN_VAY                                 AS MGT_OFCR,
        d.TP_PP_QL_KHOAN_VAY                                AS MGT_MGR,
        d.CAP_PHE_DUYET_GNTL                                AS APRV_AHR,
        a.AUTHORISER                                        AS CHKER_OFCR,
        a.INPUTTER                                          AS MAKER_OFCR,
        a.MB_RM_BANCHEO                                     AS REFRRER_OFCR,
        CASE WHEN e.CT_KHUYEN_MAI_CHO_VAY IS NULL AND i.KHOI = 'INDIV' THEN f.MB_LD_TYPE ELSE e.CT_KHUYEN_MAI_CHO_VAY END AS PROM_PRGM,
        CASE WHEN e.CTKM2 IS NULL AND i.KHOI <> 'INDIV' THEN f.MB_LD_TYPE END AS PROM_PRGM_2,
        g.CO_CAU_NO                                         AS DBT_RSTC_TP_CODE,
        TO_DATE(g.NGAY_HET_COVID19, 'yyyyMMdd')             AS COVID19_RSTC_COMPL_DT,
        h.SBV_CODE                                          AS GL_ITM_CODE,
        h.SBV_CODE                                          AS GL_ITM_ID,
        '1'                                                 AS FNC_ST_CODE,
        current_date()                                      AS SYS_EFF_DT,
        TO_DATE('9999-12-31', 'yyyy-MM-dd')                 AS SYS_EXP_DT,
        current_timestamp()                                  AS SYS_UDT_DT
    FROM demo.default.bz_t24core_ld_loans_and_deposits a
    LEFT JOIN demo.default.bz_ebanking_col_udf_value b ON ((b.COL_REF = a.ACC_FCC AND b.FUNCTION_CODE = 'CONTRACT_INFOR' AND a.RECID LIKE 'LD%') OR (b.COL_REF = a.FCC_LIMIT AND b.FUNCTION_CODE = 'LIMIT_INFOR')) AND b.UDF_ID = 'NHANNO_BATBUOC'
    LEFT JOIN demo.default.bz_flexbo_pgb_contract_udf_map c ON c.CONTRACT_REF_NO = a.ACC_FCC AND c.FIELD_NAME = 'SO LAN CO CAU'
    LEFT JOIN demo.default.bz_flexbo_pgb_contract_udf_map c1 ON c1.CONTRACT_REF_NO = a.ACC_FCC AND c1.FIELD_NAME = 'LOAI CHUNG KHOAN'
    LEFT JOIN demo.default.bz_flexbo_pgb_los_contract_fields_tdate d ON d.CONTRACT_REF_NO = a.RECID AND d.MOV_DATE = COALESCE(a.ORIG_VAL_DATE, a.VALUE_DATE)
    LEFT JOIN demo.default.bz_flexbo_pgbld_contract_udfield_hist e ON e.CONTRACT_REF_NO = a.ACC_FCC
    LEFT JOIN demo.default.bz_t24core_mb_mg_saving_multi f ON f.LD_ID = a.RECID
    LEFT JOIN demo.default.bz_flexbo_pgbld_rt_contract_udfield_hist g ON g.CONTRACT_REF_NO = a.ACC_FCC
    LEFT JOIN (SELECT DISTINCT TRANS_REF, SBV_CODE FROM demo.default.bz_source_saoke_mvmt WHERE SBV_CODE LIKE '39%') h ON h.TRANS_REF = a.RECID
    LEFT JOIN demo.default.bz_t24core_customer i ON i.RECID = a.CUSTOMER_ID
),
his_prfl AS (
    SELECT 
        sha256(concat('T24_LD_LOANS_AND_DEPOSITS', SPLIT(a.RECID, ';')[0])) AS AR_ID,
        a.INT_LIQ_ACCT                                      AS INT_LQD_AR_CODE,
        sha256(concat('T24_ACCOUNT', a.INT_LIQ_ACCT))        AS INT_LQD_AR_ID,
        a.PRIN_LIQ_ACCT                                     AS PNP_LQD_AR_CODE,
        sha256(concat('T24_ACCOUNT', a.PRIN_LIQ_ACCT))       AS PNP_LQD_AR_ID,
        a.STATUS                                            AS AR_LCS_TP_CODE,
        a.CO_CODE                                           AS OU_CODE,
        NULL                                                AS MAND_REPYMT,
        NULL                                                AS NBR_OF_DBT_RSTC,
        NULL                                                AS SCR_TP,
        NULL                                                AS CAR_AP_CODE,
        NULL                                                AS LOS_CTR_CODE,
        NULL                                                AS LOS_CTR_ID,
        NULL                                                AS DBT_WVR_F,
        NULL                                                AS AR_PPS_BY_IDY,
        NULL                                                AS AR_PPS_BY_PD,
        NULL                                                AS DSBR_PRPSL_OFCR,
        NULL                                                AS DSBR_PRPSL_MGR,
        NULL                                                AS MGT_OFCR,
        NULL                                                AS MGT_MGR,
        NULL                                                AS APRV_AHR,
        a.AUTHORISER                                        AS CHKER_OFCR,
        a.INPUTTER                                          AS MAKER_OFCR,
        a.MB_RM_BANCHEO                                     AS REFRRER_OFCR,
        NULL                                                AS PROM_PRGM,
        NULL                                                AS PROM_PRGM_2,
        NULL                                                AS DBT_RSTC_TP_CODE,
        NULL                                                AS COVID19_RSTC_COMPL_DT,
        NULL                                                AS GL_ITM_CODE,
        NULL                                                AS GL_ITM_ID,
        '1'                                                 AS FNC_ST_CODE,
        current_date()                                      AS SYS_EFF_DT,
        TO_DATE('9999-12-31', 'yyyy-MM-dd')                 AS SYS_EXP_DT,
        current_timestamp()                                  AS SYS_UDT_DT
    FROM demo.default.bz_t24core_ld_loans_and_deposits_his a
    WHERE NOT EXISTS (
        SELECT 1 FROM demo.default.bz_t24core_ld_loans_and_deposits live 
        WHERE live.RECID = SPLIT(a.RECID, ';')[0]
    )
)
SELECT * FROM live_prfl
UNION ALL
SELECT * FROM his_prfl;
