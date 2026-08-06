-- ====================================================================
-- SATELLITE 4: OU (Organization Unit - SCD Type 1)
-- Source: bz_t24core_company
-- Target: ou (Silver Layer)
-- ====================================================================

SELECT 
    sha2(concat('T24_COMPANY', RECID), 256)  AS OU_ID,
    RECID                                    AS OU_CODE,
    COMPANY_NAME                             AS OU_NM,
    current_date()                           AS SYS_EFF_DT,
    TO_DATE('9999-12-31', 'yyyy-MM-dd')      AS SYS_EXP_DT,
    current_timestamp()                       AS SYS_UDT_DT
FROM bz_t24core_company;
