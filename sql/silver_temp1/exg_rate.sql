-- Task 30: EXG_RATE (Exchange Rate)
-- Layer: Silver Temp 1 (Tech Logic)

CREATE OR REPLACE TABLE demo.default.exg_rate AS
SELECT 
    ID                                      AS FRST_CCY_CODE,
    'VND'                                   AS SCD_CCY_CODE,
    CAST(MID_REVAL_RATE AS DECIMAL(18,4))   AS MID_RATE,
    CURRENT_DATE()                          AS SNPST_DT,
    'T24_CURRENCY'                          AS SRC_STM_CODE
FROM demo.default.bz_t24_currency;
