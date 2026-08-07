-- ====================================================================
-- TASK 10: LOAN_AR (Loan Arrangement Business Silver - SCD Type 1)
-- Source: bz_t24core_ld_loans_and_deposits (Live LD)
--   UNION ALL bz_t24core_ld_loans_and_deposits_his (History LD)
--   UNION ALL bz_t24core_pd_payment_due & bz_t24core_pd_payment_due_his_mv (PD Branch)
-- Target: loan_ar (Silver Layer) - Compliant with PG_Source_PoC.xlsx mapping
-- ====================================================================

WITH live_branch AS (
    SELECT 
        sha2(concat('T24_LD_LOANS_AND_DEPOSITS', ld.RECID), 256) AS AR_ID,
        ld.RECID                                                AS AR_CODE,
        ld.ACC_FCC                                              AS FCC_AR_CODE,
        TRY_CAST(ld.APPROVE_AMOUNT AS DECIMAL(18,2))            AS ORIG_AMT_FCY,
        ld.AUTHORISER                                           AS CHKER_OFCR_CODE,
        ld.CATEGORY                                             AS PD_CGY_CODE,
        ld.CRA_LD                                               AS PPR_CTR_NBR,
        ld.CURRENCY                                             AS CCY_CODE,
        sha2(concat('T24_CUSTOMER', ld.CUSTOMER_ID), 256)       AS CST_ID,
        ld.CUSTOMER_ID                                          AS CST_CODE,
        TRY_CAST(ld.DRAWDOWN_NET_AMT AS DECIMAL(18,2))          AS DSBR_AMT_FCY,
        TRY_CAST(ld.EXCG_RATE AS DECIMAL(18,6))                 AS EXG_RATE_TO_LCL_CCY,
        ld.FCC_LIMIT                                            AS FCC_LMT_CODE,
        TO_DATE(ld.FIN_MAT_DATE, 'yyyyMMdd')                    AS MAT_DT,
        ld.INPUTTER                                             AS MAKER_OFCR_CODE,
        sha2(concat('T24_LIMIT', ld.LIMIT_REFERENCE), 256)      AS LMT_ID,
        ld.LIMIT_REFERENCE                                      AS LMT_CODE,
        ld.LOC_TERM                                             AS TERM_CODE,
        ld.LOS_FAC_CODE                                         AS LOS_CTR_NBR,
        ld.MB_RM_BANCHEO                                        AS REFRRER_OFCR_CODE,
        TO_DATE(ld.ORIG_VAL_DATE, 'yyyyMMdd')                   AS VAL_DT,
        ld.USE_OF_LOAN_CHA                                      AS AR_PPS_GRP_CODE,
        COALESCE(td.MDVAY_SP, ld.USE_OF_LOAN)                   AS AR_PPS_TP_CODE,
        TO_DATE(ld.VALUE_DATE, 'yyyyMMdd')                      AS EFF_DT,
        TO_DATE(m.BOOKING_DATE, 'yyyyMMdd')                     AS FCC_VAL_DT,
        FIRST_VALUE(ld.CO_CODE) OVER (PARTITION BY ld.RECID ORDER BY ld.VALUE_DATE) AS FRST_OU_CODE,
        CASE 
            WHEN ld.ACC_FCC IS NOT NULL AND i.KHOI = 'INDIV' THEN td.SAN_PHAM_CHO_VAY_KHCN
            WHEN ld.ACC_FCC IS NOT NULL AND i.KHOI <> 'INDIV' THEN td.SAN_PHAM_CHO_VAY_KHDN
            ELSE COALESCE(pd_los.PRODUCT_CODE, ld.PRODUCTGR_CODE)
        END                                                     AS PD_CODE,
        'T24_LD_LOANS_AND_DEPOSITS'                             AS SRC_STM_CODE,
        current_date()                                          AS SYS_EFF_DT,
        TO_DATE('9999-12-31', 'yyyy-MM-dd')                    AS SYS_EXP_DT,
        current_timestamp()                                     AS SYS_UDT_DT
    FROM bz_t24core_ld_loans_and_deposits ld
    LEFT JOIN bz_flexbo_pgb_ldtb_contract_master m ON ld.ACC_FCC = m.CONTRACT_REF_NO
    LEFT JOIN bz_flexbo_pgb_los_contract_fields_tdate td ON ld.RECID = td.CONTRACT_REF_NO AND td.MOV_DATE = COALESCE(ld.ORIG_VAL_DATE, ld.VALUE_DATE)
    LEFT JOIN bz_los_app_loan_disbursement ld_los ON ld_los.EXT_REF_NO_1 = ld.RECID AND ld_los.AMND_STATE = 'F' AND ld_los.STATUS IN ('A','U') AND ld_los.BOOKING_DATE = COALESCE(ld.ORIG_VAL_DATE, ld.VALUE_DATE)
    LEFT JOIN bz_los_app_facility los ON ld_los.FACILITY_ID = los.ID
    LEFT JOIN bz_los_app_product pd_los ON los.PRODUCT_ID = pd_los.ID
    LEFT JOIN bz_t24core_customer i ON i.RECID = ld.CUSTOMER_ID
),
his_branch AS (
    SELECT 
        sha2(concat('T24_LD_LOANS_AND_DEPOSITS', SPLIT(ldh.RECID, ';')[0]), 256) AS AR_ID,
        SPLIT(ldh.RECID, ';')[0]                                 AS AR_CODE,
        ldh.ACC_FCC                                              AS FCC_AR_CODE,
        TRY_CAST(ldh.APPROVE_AMOUNT AS DECIMAL(18,2))            AS ORIG_AMT_FCY,
        CAST(NULL AS STRING)                                     AS CHKER_OFCR_CODE,
        ldh.CATEGORY                                             AS PD_CGY_CODE,
        ldh.CRA_LD                                               AS PPR_CTR_NBR,
        ldh.CURRENCY                                             AS CCY_CODE,
        sha2(concat('T24_CUSTOMER', ldh.CUSTOMER_ID), 256)       AS CST_ID,
        ldh.CUSTOMER_ID                                          AS CST_CODE,
        TRY_CAST(ldh.DRAWDOWN_NET_AMT AS DECIMAL(18,2))          AS DSBR_AMT_FCY,
        TRY_CAST(ldh.EXCG_RATE AS DECIMAL(18,6))                 AS EXG_RATE_TO_LCL_CCY,
        CAST(NULL AS STRING)                                     AS FCC_LMT_CODE,
        TO_DATE(ldh.FIN_MAT_DATE, 'yyyyMMdd')                    AS MAT_DT,
        CAST(NULL AS STRING)                                     AS MAKER_OFCR_CODE,
        CAST(NULL AS STRING)                                     AS LMT_ID,
        CAST(NULL AS STRING)                                     AS LMT_CODE,
        ldh.LOC_TERM                                             AS TERM_CODE,
        CAST(NULL AS STRING)                                     AS LOS_CTR_NBR,
        CAST(NULL AS STRING)                                     AS REFRRER_OFCR_CODE,
        TO_DATE(ldh.ORIG_VAL_DATE, 'yyyyMMdd')                   AS VAL_DT,
        ldh.USE_OF_LOAN_CHA                                      AS AR_PPS_GRP_CODE,
        COALESCE(td.MDVAY_SP, ldh.USE_OF_LOAN)                   AS AR_PPS_TP_CODE,
        TO_DATE(ldh.VALUE_DATE, 'yyyyMMdd')                      AS EFF_DT,
        TO_DATE(m.BOOKING_DATE, 'yyyyMMdd')                     AS FCC_VAL_DT,
        ldh.CO_CODE                                              AS FRST_OU_CODE,
        CASE 
            WHEN ldh.ACC_FCC IS NOT NULL AND i.KHOI = 'INDIV' THEN td.SAN_PHAM_CHO_VAY_KHCN
            WHEN ldh.ACC_FCC IS NOT NULL AND i.KHOI <> 'INDIV' THEN td.SAN_PHAM_CHO_VAY_KHDN
            ELSE pd_los.PRODUCT_CODE
        END                                                     AS PD_CODE,
        'T24_LD_LOANS_AND_DEPOSITS_HIS'                          AS SRC_STM_CODE,
        current_date()                                           AS SYS_EFF_DT,
        TO_DATE('9999-12-31', 'yyyy-MM-dd')                        AS SYS_EXP_DT,
        current_timestamp()                                      AS SYS_UDT_DT
    FROM bz_t24core_ld_loans_and_deposits_his ldh
    LEFT JOIN bz_flexbo_pgb_ldtb_contract_master m ON ldh.ACC_FCC = m.CONTRACT_REF_NO
    LEFT JOIN bz_flexbo_pgb_los_contract_fields_tdate td ON SPLIT(ldh.RECID, ';')[0] = td.CONTRACT_REF_NO AND td.MOV_DATE = COALESCE(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)
    LEFT JOIN bz_los_app_loan_disbursement ld_los ON ld_los.EXT_REF_NO_1 = SPLIT(ldh.RECID, ';')[0] AND ld_los.AMND_STATE = 'F' AND ld_los.STATUS IN ('A','U') AND ld_los.BOOKING_DATE = COALESCE(ldh.ORIG_VAL_DATE, ldh.VALUE_DATE)
    LEFT JOIN bz_los_app_facility los ON ld_los.FACILITY_ID = los.ID
    LEFT JOIN bz_los_app_product pd_los ON los.PRODUCT_ID = pd_los.ID
    LEFT JOIN bz_t24core_customer i ON i.RECID = ldh.CUSTOMER_ID
    WHERE NOT EXISTS (
        SELECT 1 FROM bz_t24core_ld_loans_and_deposits live 
        WHERE live.RECID = SPLIT(ldh.RECID, ';')[0]
    )
),
pd_branch AS (
    SELECT 
        sha2(concat('T24_LD_LOANS_AND_DEPOSITS', SUBSTR(d.RECID, 1, 14)), 256) AS AR_ID,
        d.RECID                                                 AS AR_CODE,
        d1.ACC_FCC                                              AS FCC_AR_CODE,
        TRY_CAST(d1.APPROVE_AMOUNT AS DECIMAL(18,2))            AS ORIG_AMT_FCY,
        d.AUTHORISER                                            AS CHKER_OFCR_CODE,
        d1.CATEGORY                                             AS PD_CGY_CODE,
        d.CRA_LD                                                AS PPR_CTR_NBR,
        CAST(NULL AS STRING)                                    AS CCY_CODE,
        sha2(concat('T24_CUSTOMER', d.CUSTOMER), 256)           AS CST_ID,
        d.CUSTOMER                                              AS CST_CODE,
        CAST(NULL AS DECIMAL(18,2))                             AS DSBR_AMT_FCY,
        CAST(NULL AS DECIMAL(18,6))                             AS EXG_RATE_TO_LCL_CCY,
        CAST(NULL AS STRING)                                    AS FCC_LMT_CODE,
        TO_DATE(d1.FINAL_DUE_DATE, 'yyyyMMdd')                  AS MAT_DT,
        d.INPUTTER                                              AS MAKER_OFCR_CODE,
        sha2(concat('T24_LIMIT', d.LIMIT_REFERENCE), 256)       AS LMT_ID,
        d.LIMIT_REFERENCE                                       AS LMT_CODE,
        CAST(NULL AS STRING)                                    AS TERM_CODE,
        d.LOS_FAC_CODE                                          AS LOS_CTR_NBR,
        CAST(NULL AS STRING)                                    AS REFRRER_OFCR_CODE,
        CAST(NULL AS DATE)                                      AS VAL_DT,
        CAST(NULL AS STRING)                                    AS AR_PPS_GRP_CODE,
        CAST(NULL AS STRING)                                    AS AR_PPS_TP_CODE,
        TO_DATE(SUBSTR(d.PAYMENT_DTE_DUE, 1, 8), 'yyyyMMdd')     AS EFF_DT,
        CAST(NULL AS DATE)                                      AS FCC_VAL_DT,
        d1.CO_CODE                                              AS FRST_OU_CODE,
        CAST(NULL AS STRING)                                    AS PD_CODE,
        'T24_PD_PAYMENT_DUE'                                    AS SRC_STM_CODE,
        current_date()                                          AS SYS_EFF_DT,
        TO_DATE('9999-12-31', 'yyyy-MM-dd')                    AS SYS_EXP_DT,
        current_timestamp()                                     AS SYS_UDT_DT
    FROM bz_t24core_pd_payment_due d
    LEFT JOIN bz_t24core_pd_payment_due_his_mv d1 ON d.RECID = d1.RECID
    WHERE NOT EXISTS (
        SELECT 1 FROM bz_t24core_ld_loans_and_deposits live 
        WHERE live.RECID = SUBSTR(d.RECID, 1, 14)
    )
)
SELECT * FROM live_branch
UNION ALL
SELECT * FROM his_branch
UNION ALL
SELECT * FROM pd_branch;
