-- Task 35: AR_RATE_HIST (Arrangement Rate History)
-- Layer: Silver Temp 1 (Tech Logic)

CREATE OR REPLACE TABLE demo.default.ar_rate_hist AS
SELECT 
    hash("T24_LD_LOANS_AND_DEPOSITS", a.RECID)    AS AR_ID,
    a.RECID                                        AS AR_CODE,
    a.INT_RATE_TYPE                                AS INT_RATE_VARIABLITY_TP_CODE,
    a.INT_VALUE_DATE                               AS INT_RATE_EFF_DT,
    a.INTEREST_RATE                                AS EFF_RATE_PCT,
    a.INTEREST_SPREAD                              AS SPRD_RATE_PCT,
    a.PE_RATE                                      AS PNY_RATE,
    a.PS_RATE                                      AS ODUE_INT_RATE,
    d.BIEN_DO_THAY_DOI_LS_THEO_HDTD                AS MRGN_RATE
FROM demo.default.bz_t24_ld_loans_and_deposits a
LEFT JOIN demo.default.bz_flexbo_pgb_los_contract_fields_tdate d ON d.CONTRACT_REF_NO = a.RECID;
