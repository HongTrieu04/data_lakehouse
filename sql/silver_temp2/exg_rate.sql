-- ====================================================================
-- SATELLITE 5: EXG_RATE / EXG_RATE_HIST (Exchange Rate - Latest / History)
-- Source: bz_pg_t24core_currency UNION ALL bz_pg_t24core_currency_his
-- Target: demo.default.exg_rate (Silver Layer)
-- ====================================================================

WITH cur_live AS (
    SELECT 
        ID                                              AS FRST_CCY_CODE,
        'VND'                                           AS SCD_CCY_CODE,
        CAST(SPLIT(MID_REVAL_RATE, '#')[0] AS DECIMAL(18,6)) AS MID_RATE,
        DATE_TIME                                       AS DATE_TIME_VAL,
        999999                                          AS CURR_NO
    FROM demo.default.bz_pg_t24core_currency
),
cur_his AS (
    SELECT 
        SPLIT(ID, ';')[0]                               AS FRST_CCY_CODE,
        'VND'                                           AS SCD_CCY_CODE,
        CAST(SPLIT(MID_REVAL_RATE, '#')[0] AS DECIMAL(18,6)) AS MID_RATE,
        DATE_TIME                                       AS DATE_TIME_VAL,
        CAST(CURR_NO AS INT)                            AS CURR_NO
    FROM demo.default.bz_pg_t24core_currency_his
),
cur_combined AS (
    SELECT * FROM cur_live
    UNION ALL
    SELECT * FROM cur_his
),
cur_ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY FRST_CCY_CODE ORDER BY CURR_NO DESC, DATE_TIME_VAL DESC) as rn
    FROM cur_combined
)
SELECT 
    current_date()                                      AS SNPST_DT,
    'T24_CURRENCY'                                      AS SRC_STM_CODE,
    FRST_CCY_CODE,
    SCD_CCY_CODE,
    MID_RATE,
    current_timestamp()                                 AS SYS_UDT_DT
FROM cur_ranked
WHERE rn = 1;
