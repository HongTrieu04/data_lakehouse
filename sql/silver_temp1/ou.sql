-- Task 17: OU (Organization Unit)
-- Layer: Silver Temp 1 (Tech Logic)

CREATE OR REPLACE TABLE demo.default.ou AS
SELECT 
    hash("T24_COMPANY", RECID)      AS OU_ID,
    RECID                           AS OU_CODE,
    COMPANY_NAME                    AS OU_NM,
    CURRENT_TIMESTAMP()             AS SYS_EFF_DT,
    TIMESTAMP('9999-12-31 23:59:59') AS SYS_EXP_DT,
    CURRENT_TIMESTAMP()             AS SYS_UDT_DT
FROM demo.default.bz_t24_company;
