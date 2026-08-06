# KẾ HOẠCH TRIỂN KHAI ĐỘC LẬP — TRACK 2: LOAN CONTRACT DOMAIN (TASK 9 → TASK 12)

> **Bối cảnh bắt buộc đọc trước khi thực thi:** Task 9–12 (Fresher 2) được thực hiện **độc lập hoàn toàn**, KHÔNG được kế thừa bất kỳ bảng Silver nào đã được tạo sẵn bởi các track khác (Track 3 – OU, Track 5 – EXG_RATE, Track 6 – AR_BAL/AR_RATE_HIST/AR_DLQ_SMY/AST_AR_INT_SMY). Vì `INTF_LOAN_AR` (task 9) cần join tới 6 bảng Silver "vệ tinh" này, ta phải **tự build lại toàn bộ 6 bảng vệ tinh đó từ Source → Bronze → Silver** trong phạm vi của track này, trước khi build INTF_LOAN_AR.
>
> Nguyên tắc: **không có bước nào được skip hoặc "giả định đã tồn tại"**. Mọi bảng đích trong pipeline này phải có một job cụ thể load nó từ tầng thấp hơn.

---

## 0. Kiến trúc & nguyên tắc chung

```
Source (hệ thống lõi: T24CORE, FLEXBO, LOS_APP, EBANKING, SOURCE, PG_T24CORE)
   │  (Landing / Extract - Full hoặc CDC theo ETL_DATE)
   ▼
Bronze (1:1 với Source, thêm audit columns: LOAD_DATE, ETL_DATE, SRC_FILE...)
   │  (Technical transform: type casting, standardize, hash key)
   ▼
Silver Temp 1 (Tech logic) — nếu bảng đích có logic kỹ thuật thuần túy
   │  (Business transform: bank rule, SCD1/SCD2, filter nghiệp vụ)
   ▼
Silver Temp 2 (Business logic) — bảng nghiệp vụ hoàn chỉnh theo domain
   │  (Join nhiều Silver Temp2 lại — cross-domain)
   ▼
Interface / Gold — bảng xuất báo cáo hoặc Dimension/Fact cho Data Mart
```

- **fn_hash("SYSTEM_TABLE", KEY)**: hàm sinh khóa surrogate dùng chung toàn hệ thống (VD `fn_hash("T24_LD_LOANS_AND_DEPOSITS", RECID)`), phải triển khai nhất quán ở mọi bảng dùng chung khóa `AR_ID`, `CST_ID`, `LMT_ID`...
- **ETL_DATE / ${ETL_DATE}**: biến ngày chạy batch (business date), dùng để lọc CDC và tính các cột toán ngày (số ngày quá hạn...).
- **SCD Type 1**: ghi đè, không giữ lịch sử (áp dụng cho `LOAN_AR`).
- **SCD Type 2**: giữ lịch sử qua `SYS_EFF_DT` / `SYS_EXP_DT` / `SYS_UDT_DT` (áp dụng cho `LOAN_AR_PRFL`, `DIM_LOAN_AR`, và các bảng vệ tinh `AR_RATE_HIST`, `AR_DLQ_SMY` bản chất SCD2/snapshot theo ngày).

---

## 1. THỨ TỰ THỰC THI BẮT BUỘC (Execution Order)

Vì không được kế thừa, thứ tự **duy nhất hợp lệ** để không bị thiếu dependency là:

```
PHASE 0 : Source -> Bronze cho TOÀN BỘ 21 bảng nguồn (song song, không phụ thuộc nhau)
PHASE 1 : Bronze -> Silver cho 6 bảng "vệ tinh" (tự build lại, song song với nhau)
            AR_BAL | AR_RATE_HIST | AR_DLQ_SMY | OU | EXG_RATE | AST_AR_INT_SMY
PHASE 2 : Bronze -> Silver Temp2  ->  LOAN_AR          (Task 10)
PHASE 3 : Bronze -> Silver Temp2  ->  LOAN_AR_PRFL      (Task 11)
PHASE 4 : Silver(8 bảng: 6 vệ tinh + LOAN_AR + LOAN_AR_PRFL) -> INTF_LOAN_AR   (Task 9)
PHASE 5 : Silver(LOAN_AR + LOAN_AR_PRFL) -> DIM_LOAN_AR                        (Task 12)
```

**Lưu ý dependency:**
- Phase 2 và Phase 3 có thể chạy **song song** (đều chỉ phụ thuộc Phase 0).
- Phase 1 có thể chạy **song song** với Phase 2, Phase 3 (không phụ thuộc nhau).
- Phase 4 (`INTF_LOAN_AR`) phải **chờ cả Phase 1, Phase 2, Phase 3 hoàn tất**.
- Phase 5 (`DIM_LOAN_AR`) chỉ cần chờ Phase 2, Phase 3 (không phụ thuộc Phase 1/Phase 4).
- Airflow: dùng `TriggerRule.ALL_SUCCESS` cho các task chờ nhiều nhánh; có thể dùng `ExternalTaskSensor` nếu tách DAG, hoặc gom hết vào 1 DAG `dag_track2_loan_standalone.py` với TaskGroup theo Phase nếu chạy trong cùng 1 DAG.

---

## 2. DANH SÁCH ĐẦY ĐỦ BẢNG NGUỒN (SOURCE) CẦN ĐỌC — 21 BẢNG

### Nhóm A — Nguồn phục vụ trực tiếp `LOAN_AR` / `LOAN_AR_PRFL` (14 bảng)

| # | Hệ thống | Bảng nguồn | Dùng cho |
|---|---|---|---|
| A1 | T24CORE | `LD_LOANS_AND_DEPOSITS` | LOAN_AR, LOAN_AR_PRFL, AR_RATE_HIST |
| A2 | T24CORE | `LD_LOANS_AND_DEPOSITS_HIS` | LOAN_AR, LOAN_AR_PRFL (nhánh UNION ALL khởi tạo) |
| A3 | FLEXBO | `PGB_LDTB_CONTRACT_MASTER` | LOAN_AR |
| A4 | FLEXBO | `PGB_LOS_CONTRACT_FIELDS_TDATE` | LOAN_AR, LOAN_AR_PRFL, AR_RATE_HIST |
| A5 | LOS_APP | `LOAN_DISBURSEMENT@TO_LOS` | LOAN_AR |
| A6 | LOS_APP | `FACILITY@TO_LOS` | LOAN_AR |
| A7 | LOS_APP | `PRODUCT@TO_LOS` | LOAN_AR |
| A8 | T24CORE | `CUSTOMER` | LOAN_AR, LOAN_AR_PRFL |
| A9 | EBANKING | `COL_UDF_VALUE@PGDB` | LOAN_AR_PRFL |
| A10 | FLEXBO | `PGB_CONTRACT_UDF_MAP` | LOAN_AR_PRFL (2 lần: field `SO LAN CO CAU`, `LOAI CHUNG KHOAN`) |
| A11 | FLEXBO | `PGBLD_CONTRACT_UDFIELD_HIST` | LOAN_AR_PRFL |
| A12 | T24CORE | `MB_MG_SAVING_MULTI` | LOAN_AR_PRFL |
| A13 | FLEXBO | `PGBLD_RT_CONTRACT_UDFIELD_HIST` | LOAN_AR_PRFL |
| A14 | SOURCE | `SAOKE_MVMT` | LOAN_AR_PRFL |

### Nhóm B — Nguồn phục vụ 6 bảng "vệ tinh" phải tự build lại (7 bảng)

| # | Hệ thống | Bảng nguồn | Dùng để build vệ tinh |
|---|---|---|---|
| B1 | SOURCE | `SAOKE_CRB` | AR_BAL, AST_AR_INT_SMY |
| B2 | T24CORE | `STMT_ENTRY` | AST_AR_INT_SMY |
| B3 | T24CORE | `PD_PAYMENT_DUE_HIS_MV` | AR_DLQ_SMY |
| B4 | T24CORE | `PD_PAYMENT_DUE` | AR_DLQ_SMY |
| B5 | T24CORE | `COMPANY` | OU |
| B6 | PG_T24CORE | `CURRENCY` | EXG_RATE |
| B7 | PG_T24CORE | `CURRENCY_HIS` | EXG_RATE |

> `AR_RATE_HIST` không cần nguồn mới — dùng lại A1 (`LD_LOANS_AND_DEPOSITS`) + A4 (`PGB_LOS_CONTRACT_FIELDS_TDATE`) đã landing ở Nhóm A.

**⚠️ Điểm chưa chắc chắn cần xác nhận với BA/DE lead trước khi code:** trong sheet mapping `Interface Loan Arrangement`, alias join dùng tên `EXG_RATE_HIST` (không phải `EXG_RATE`). File Excel gốc không có sheet/DDL riêng cho `EXG_RATE_HIST`. Giả định làm việc trong plan này: `EXG_RATE_HIST` là **cùng bảng vật lý** với `EXG_RATE` (sheet `Exchange Rate`), có thể khác tên alias do lịch sử đặt tên. Cần agent/BA xác nhận lại DDL thật trong DB trước khi build Phase 1.

---

## 3. PHASE 0 — Source → Bronze (Landing)

Tạo 21 bảng Bronze, mỗi bảng 1:1 schema với Source, cộng thêm cột audit chuẩn:
`BRZ_LOAD_DT` (ngày landing), `SRC_SYSTEM` (tên hệ thống nguồn), `ETL_BATCH_ID`.

| STT | Source Table | Bronze Table (đề xuất) | Chiến lược load |
|---|---|---|---|
| A1 | T24CORE.LD_LOANS_AND_DEPOSITS | BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS | Full/Incremental theo `VALUE_DATE`/CDC |
| A2 | T24CORE.LD_LOANS_AND_DEPOSITS_HIS | BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS_HIS | Append-only (bảng lịch sử) |
| A3 | FLEXBO.PGB_LDTB_CONTRACT_MASTER | BRONZE.FLEXBO_PGB_LDTB_CONTRACT_MASTER | Full/Incremental theo `BOOKING_DATE` |
| A4 | FLEXBO.PGB_LOS_CONTRACT_FIELDS_TDATE | BRONZE.FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE | Incremental theo `MOV_DATE` |
| A5 | LOS_APP.LOAN_DISBURSEMENT@TO_LOS | BRONZE.LOS_APP_LOAN_DISBURSEMENT | Incremental theo `BOOKING_DATE` |
| A6 | LOS_APP.FACILITY@TO_LOS | BRONZE.LOS_APP_FACILITY | Full |
| A7 | LOS_APP.PRODUCT@TO_LOS | BRONZE.LOS_APP_PRODUCT | Full (master data, ít thay đổi) |
| A8 | T24CORE.CUSTOMER | BRONZE.T24CORE_CUSTOMER | Full/Incremental |
| A9 | EBANKING.COL_UDF_VALUE@PGDB | BRONZE.EBANKING_COL_UDF_VALUE | Incremental, filter `UDF_ID='NHANNO_BATBUOC'` có thể để ở Silver |
| A10 | FLEXBO.PGB_CONTRACT_UDF_MAP | BRONZE.FLEXBO_PGB_CONTRACT_UDF_MAP | Full/Incremental |
| A11 | FLEXBO.PGBLD_CONTRACT_UDFIELD_HIST | BRONZE.FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST | Append-only |
| A12 | T24CORE.MB_MG_SAVING_MULTI | BRONZE.T24CORE_MB_MG_SAVING_MULTI | Full/Incremental |
| A13 | FLEXBO.PGBLD_RT_CONTRACT_UDFIELD_HIST | BRONZE.FLEXBO_PGBLD_RT_CONTRACT_UDFIELD_HIST | Append-only |
| A14 | SOURCE.SAOKE_MVMT | BRONZE.SOURCE_SAOKE_MVMT | Incremental theo ngày giao dịch |
| B1 | SOURCE.SAOKE_CRB | BRONZE.SOURCE_SAOKE_CRB | Snapshot hàng ngày (filter `APP_CODE IN ('LD','PD')` có thể để ở Silver) |
| B2 | T24CORE.STMT_ENTRY | BRONZE.T24CORE_STMT_ENTRY | Incremental theo `booking_date` |
| B3 | T24CORE.PD_PAYMENT_DUE_HIS_MV | BRONZE.T24CORE_PD_PAYMENT_DUE_HIS_MV | Incremental theo `DATE_TIME` |
| B4 | T24CORE.PD_PAYMENT_DUE | BRONZE.T24CORE_PD_PAYMENT_DUE | Full/Incremental |
| B5 | T24CORE.COMPANY | BRONZE.T24CORE_COMPANY | Full (master data) |
| B6 | PG_T24CORE.CURRENCY | BRONZE.T24CORE_CURRENCY | Incremental theo `DATE_TIME` |
| B7 | PG_T24CORE.CURRENCY_HIS | BRONZE.T24CORE_CURRENCY_HIS | Append-only |

**Recon Phase 0 (bắt buộc cho từng bảng):** Row count Source vs Bronze theo batch ETL_DATE; kiểm tra NULL ở khóa chính (RECID/APP_ID/RECID...).

---

## 4. PHASE 1 — Build lại 6 bảng vệ tinh (Silver, KHÔNG kế thừa)

### 4.1 `AR_BAL` (Arrangement Balance) — TYPE: SNAPSHOT

- **Nguồn Bronze:** `BRONZE.SOURCE_SAOKE_CRB` (alias `a`)
- **Filter:** `WHERE APP_CODE IN ('LD', 'PD')`
- **Transform chính:**
  - `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", APP_ID)`
  - `CST_ID = fn_hash("T24_CUSTOMER", CUSTOMER_ID)`
  - Map 1:1: `AMOUNT_CUR→RCVB_AMT_LCY`, `AMOUNT_LCY→CLS_BAL_LCY`, `APP_CODE→MUD_CODE`, `APP_ID→AR_CODE`, `ASSET_TYPE→AST_TP_CODE`, `CATEG_CODE→PD_CGY_CODE`, `CO_CODE→OU_CODE`, `CURR→CCY_CODE`, `CUS_ID→CST_CODE`, `LOC_TERM→AR_TERM_TP_CODE`, `SBV_CODE→GL_ITM_CODE/GL_ITM_ID`, `SUB_PRO→SUB_PD_CODE`, `TXN_DATE→CDR_DT`
- **Target:** `SILVER.AR_BAL`
- **Recon:** Agg sum(`RCVB_AMT_LCY`, `CLS_BAL_LCY`) & Minus test so với `SAOKE_CRB` gốc.

### 4.2 `AR_RATE_HIST` (Arrangement Rate History) — TYPE: SCD TYPE 2

- **Nguồn Bronze:** `BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS` (alias `a`) **left join** `BRONZE.FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE` (alias `d`)
  `ON d.CONTRACT_REF_NO = a.RECID AND d.MOV_DATE = TO_DATE(COALESCE(a.ORIG_VAL_DATE, a.VALUE_DATE), 'YYYYMMDD')`
- **Transform chính:**
  - `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", RECID)`, `AR_CODE = RECID`
  - Map 1:1: `INT_RATE_TYPE→INT_RATE_VARIABLITY_TP_CODE`, `INT_VALUE_DATE→INT_RATE_EFF_DT`, `INTEREST_RATE→EFF_RATE_PCT`, `INTEREST_SPREAD→SPRD_RATE_PCT`, `PE_RATE→PNY_RATE`, `PS_RATE→ODUE_INT_RATE`
  - Từ `d`: `BIEN_DO_THAY_DOI_LS_THEO_HDTD → MRGN_RATE`
- **Target:** `SILVER.AR_RATE_HIST`
- **Recon:** Row count so với số lần thay đổi lãi suất trên T24.

### 4.3 `AR_DLQ_SMY` (Arrangement Delinquency Summary) — TYPE: Snapshot theo ngày

- **Nguồn Bronze:** `BRONZE.T24CORE_PD_PAYMENT_DUE_HIS_MV` (alias `a`) **left join** `BRONZE.T24CORE_PD_PAYMENT_DUE` (alias `b`) `ON a.recid = b.recid`
- **Filter trên `a`:**
  ```sql
  WHERE a.flag_status = 'LIVE'
    AND a.PAY_TYPE IN ('PR','IN')
    AND NVL(pd_type,0) <> 1
    AND a.RECORD_STATUS IS NULL
    AND NVL(a.RECORD_STATUS,'_') <> 'REVE'
    AND a.PAY_AMT_OUTS > 0
    AND a.DATE_TIME = (SELECT MAX(b.DATE_TIME) FROM BRONZE.T24CORE_PD_PAYMENT_DUE_HIS_MV b
                        WHERE b.RECID = a.RECID AND a.PAY_TYPE = b.PAY_TYPE)
    AND a.CURR_NO = (SELECT MAX(b.CURR_NO) FROM BRONZE.T24CORE_PD_PAYMENT_DUE_HIS_MV b
                      WHERE b.RECID = a.RECID AND a.PAY_TYPE = b.PAY_TYPE)
  ```
- **Transform chính (GROUP BY `SUBSTR(a.RECID,1,14)`):**
  - `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", SUBSTR(a.RECID,1,14))`; `AR_CODE = SUBSTR(a.RECID,1,14)`
  - `ORIG_AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", CASE WHEN RECID LIKE 'PDLD%' THEN SUBSTR(a.RECID,3,12) ELSE SUBSTR(a.RECID,1,14) END)`
  - `PNP_PAST_DUE_DT = MIN(CASE WHEN PAY_TYPE='PR' AND CATEGORY<>21069 THEN TO_DATE(PAYMENT_DTE_DUE,'rrrrmmdd') END)`
  - `INT_PAST_DUE_DT = MIN(CASE WHEN PAY_TYPE='IN' OR (CATEGORY=21069 AND PAY_TYPE='PR') THEN TO_DATE(PAYMENT_DTE_DUE,'rrrrmmdd') END)`
  - `PNP_ARS = MIN(CASE WHEN PAY_TYPE='PR' AND CATEGORY<>21069 THEN PAY_AMT_OUTS END)`
  - `INT_ARS = MIN(CASE WHEN PAY_TYPE='IN' OR (CATEGORY=21069 AND PAY_TYPE='PR') THEN PAY_AMT_OUTS END)`
  - `ADDITION_DYS_IN_ARS = MAX(b.SNQH_CHUYENDOI)`
- **Target:** `SILVER.AR_DLQ_SMY`
- **Recon:** Checksum overdue balances per DPD bucket.
- **Ghi chú cho `INTF_LOAN_AR` (Phase 4):** cần thêm 1 view/subquery tổng hợp `AR_DLQ_SMY` theo `ORIG_AR_ID` để tính `SO_NGAY_QH_GOC`, `SO_NGAY_QH_LAI`, `SONGAY_QHAN_CAONHAT`, `SNQH_CHUYENDOI` (xem chi tiết Phase 4).

### 4.4 `OU` (Organization Unit) — TYPE: SCD TYPE 1

- **Nguồn Bronze:** `BRONZE.T24CORE_COMPANY`
- **Transform chính:**
  - `OU_ID = fn_hash("T24_COMPANY", RECID)`; `OU_CODE = RECID`; `OU_NM = COMPANY_NAME`
  - `SYS_EFF_DT`: ETL_DATE nếu OU_ID chưa tồn tại; `SYS_EXP_DT`: ETL_DATE nếu RECID bị xoá; `SYS_UDT_DT`: ETL_DATE
- **Target:** `SILVER.OU`
- **Recon:** Minus test OU master.

### 4.5 `EXG_RATE` (Exchange Rate — dùng làm `EXG_RATE_HIST` trong Phase 4)

- **Nguồn Bronze:** `BRONZE.T24CORE_CURRENCY` (alias `CUR`) UNION `BRONZE.T24CORE_CURRENCY_HIS` (alias `CUR_HIS`), qua 3 bước subquery:
  1. `cur`: `SELECT id AS FRST_CCY_CODE, 'VND' AS SCD_CCY_CODE, COALESCE(CAST(REGEXP_SUBSTR(MID_REVAL_RATE,'[^#]+') AS int),1) AS mid_reval_rate, DATE_TIME AS date_time_value, 999999 AS curr_no, RANK() OVER (PARTITION BY id ORDER BY DATE_TIME DESC) AS rn FROM BRONZE.T24CORE_CURRENCY WHERE TO_TIMESTAMP(TO_CHAR(DATE_TIME),'YYMMDDHH24MI') < etl_date+1`
  2. `cur_his`: tương tự trên `BRONZE.T24CORE_CURRENCY_HIS`, lấy `curr_no` thật, `FRST_CCY_CODE = REGEXP_SUBSTR(id,'[^;]+')`
  3. `cur_main`: UNION ALL `cur (rn=1)` + `cur_his (rn=1)`, `RANK() OVER (PARTITION BY FRST_CCY_CODE ORDER BY curr_no DESC)`, lấy `rank=1`
- **Transform:** `SNPST_DT = etl_date`, `SRC_STM_CODE='T24_CURRENCY'`, `FRST_CCY_CODE`, `SCD_CCY_CODE='VND'`, `MID_RATE = mid_reval_rate`
- **Target:** `SILVER.EXG_RATE`
- **Recon:** Minus test Currency vs Exchange Rate.
- **⚠️ Xác nhận lại tên bảng đích thật trong DB** — sheet gốc dùng `EXG_RATE`, nhưng `Interface Loan Arrangement` join tới `EXG_RATE_HIST`. Có thể là 2 bảng khác nhau (1 snapshot mới nhất, 1 full lịch sử) — nếu đúng vậy cần thêm bước build `EXG_RATE_HIST` = bỏ điều kiện `rank=1`, giữ toàn bộ lịch sử tỷ giá.

### 4.6 `AST_AR_INT_SMY` (Asset Arrangement Interest Summary)

- **Nguồn Bronze:** `BRONZE.SOURCE_SAOKE_CRB` (alias `a`, filter `APP_CODE IN ('LD','PD')`) **left join** subquery trên `BRONZE.T24CORE_STMT_ENTRY` (alias `b`):
  ```sql
  ON a.APP_ID = b.RECID
  -- subquery b:
  SELECT SUM(CASE WHEN OUR_REFERENCE LIKE 'PDPD%' AND TRANSACTION_CODE IN ('751','434')
                   THEN DECODE(CURRENCY,'VND',ABS(AMOUNT_LCY),ABS(AMOUNT_FCY)) ELSE 0 END)
       + SUM(CASE WHEN (OUR_REFERENCE LIKE 'PDLD%' OR OUR_REFERENCE LIKE 'LD%')
                   AND CRF_PROD_CAT='21069' AND TRANSACTION_CODE IN ('750','420')
                   THEN DECODE(CURRENCY,'VND',ABS(AMOUNT_LCY),ABS(AMOUNT_FCY)) ELSE 0 END) AS SUM_REPAID_AMT,
         (CASE WHEN OUR_REFERENCE LIKE 'LD%' OR OUR_REFERENCE LIKE 'PD%' THEN OUR_REFERENCE END) AS RECID
  FROM BRONZE.T24CORE_STMT_ENTRY
  WHERE RECID NOT LIKE 'F%'
  GROUP BY (CASE WHEN OUR_REFERENCE LIKE 'LD%' OR OUR_REFERENCE LIKE 'PD%' THEN OUR_REFERENCE END)
  ```
- **Transform chính:**
  - `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", APP_ID)`; `AR_CODE = APP_ID`
  - `ORIG_AR_ID/ORIG_AR_CODE`: xử lý `CASE WHEN APP_ID LIKE 'PDLD%' THEN SUBSTR(APP_ID,3,12) ELSE APP_ID END`
  - `TOT_ACR_INT_AMT_FCY = ABS(AMOUNT_CUR)`
  - `TOT_INT_PAID_AMT_FCY = NVL(SUM_REPAID_AMT,0)` (từ `b`)
  - `TOT_INT_DUE_AMT_FCY = CASE WHEN SBV_CODE LIKE '394%' THEN ABS(NVL(AMOUNT_CUR,0)) ELSE 0 END`
  - `TOT_INT_ODUE_AMT_FCY = CASE WHEN SBV_CODE LIKE '94%' THEN ABS(NVL(AMOUNT_CUR,0)) ELSE 0 END`
- **Target:** `SILVER.AST_AR_INT_SMY`
- **Recon:** Agg sum(`TOT_ACR_INT_AMT_FCY`).
- **Ghi chú cho `INTF_LOAN_AR` (Phase 4):** cần 1 subquery tổng hợp theo `ORIG_AR_ID` (SUM 4 cột amount) trước khi join vào INTF_LOAN_AR.

---

## 5. PHASE 2 — Task 10: `LOAN_AR` (Silver Temp2 — Business, SCD Type 1)

**2 nhánh UNION ALL:**

**Nhánh chính** — từ bảng live:
```
BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS  ld
  LEFT JOIN BRONZE.FLEXBO_PGB_LDTB_CONTRACT_MASTER  m
    ON ld.ACC_FCC = m.CONTRACT_REF_NO
   AND m.VERSION_NO = (SELECT MAX(VERSION_NO) FROM ... WHERE CONTRACT_REF_NO = m.CONTRACT_REF_NO)
  LEFT JOIN BRONZE.FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE  td
    ON ld.RECID = td.CONTRACT_REF_NO
   AND td.MOV_DATE = TO_DATE(COALESCE(ld.ORIG_VAL_DATE, ld.VALUE_DATE), 'YYYYMMDD')
  LEFT JOIN BRONZE.LOS_APP_LOAN_DISBURSEMENT  ld_los
    ON ld_los.EXT_REF_NO_1 = ld.RECID AND ld_los.AMND_STATE = 'F'
   AND ld_los.STATUS IN ('A','U')
   AND ld_los.BOOKING_DATE = TO_DATE(COALESCE(ld.ORIG_VAL_DATE, ld.VALUE_DATE), 'YYYYMMDD')
  LEFT JOIN BRONZE.LOS_APP_FACILITY  los  ON ld_los.FACILITY_ID = los.ID
  LEFT JOIN BRONZE.LOS_APP_PRODUCT  pd_los  ON los.PRODUCT_ID = pd_los.ID
  LEFT JOIN BRONZE.T24CORE_CUSTOMER  i  ON i.RECID = ld.CUSTOMER_ID
```

**Nhánh khởi tạo lịch sử** — từ bảng HIS (chỉ lấy record đã đóng/không tồn tại ở bảng live):
```
BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS_HIS  ldh
WHERE ldh.CURR_NO = (SELECT MAX(CURR_NO) FROM ... WHERE SUBSTR(RECID,1,INSTR(RECID,';')-1) tương ứng)
  AND NOT EXISTS (record có RECORD_STATUS='REVE')
  AND NOT EXISTS (record vẫn còn tồn tại ở bảng LD_LOANS_AND_DEPOSITS live)
LEFT JOIN tương tự (m1, td, ld_los, los, pd_los, i) như nhánh trên, dùng SUBSTR(ldh.RECID,1,INSTR(...)-1) làm khóa join
```

**Transform / Business rule chính:**
- `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", RECID)` (nhánh HIS dùng `SUBSTR(RECID,...)` trước khi hash)
- `PD_CODE`: `CASE WHEN ld.ACC_FCC IS NOT NULL AND i.KHOI='INDIV' THEN td.SAN_PHAM_CHO_VAY_KHCN WHEN ld.ACC_FCC IS NOT NULL AND i.KHOI<>'INDIV' THEN td.SAN_PHAM_CHO_VAY_KHDN ELSE NVL(pd_los.PRODUCT_CODE, ld.PRODUCTGR_CODE) END`
- `AR_PPS_TP_CODE = NVL(td.MDVAY_SP, ld.USE_OF_LOAN)`
- `CST_ID = fn_hash("T24_CUSTOMER", CUSTOMER_ID)`; `LMT_ID = fn_hash("T24_LIMIT", LIMIT_REFERENCE)`
- `FRST_OU_CODE = FIRST_VALUE(CO_CODE) OVER (PARTITION BY RECID ORDER BY VALUE_DATE)`
- `FCC_VAL_DT = m.BOOKING_DATE`
- Map 1:1 còn lại: `RECID→AR_CODE`, `ACC_FCC→FCC_AR_CODE`, `APPROVE_AMOUNT→ORIG_AMT_FCY`, `AUTHORISER→CHKER_OFCR_CODE`, `CATEGORY→PD_CGY_CODE`, `CRA_LD→PPR_CTR_NBR`, `CURRENCY→CCY_CODE`, `CUSTOMER_ID→CST_CODE`, `DRAWDOWN_NET_AMT→DSBR_AMT_FCY`, `EXCG_RATE→EXG_RATE_TO_LCL_CCY`, `FCC_LIMIT→FCC_LMT_CODE`, `FIN_MAT_DATE→MAT_DT`, `INPUTTER→MAKER_OFCR_CODE`, `LIMIT_REFERENCE→LMT_CODE`, `LOC_TERM→TERM_CODE`, `LOS_FAC_CODE→LOS_CTR_NBR`, `MB_RM_BANCHEO→REFRRER_OFCR_CODE`, `ORIG_VAL_DATE→VAL_DT`, `USE_OF_LOAN_CHA→AR_PPS_GRP_CODE`, `VALUE_DATE→EFF_DT`
- `SRC_STM_CODE`: literal `'T24_LD_LOANS_AND_DEPOSITS'` (nhánh chính) hoặc `'T24_LD_LOANS_AND_DEPOSITS_HIS'` (nhánh HIS)
- `SYS_EFF_DT = ETL_DATE nếu RECID chưa tồn tại`; `SYS_EXP_DT = ETL_DATE nếu RECID bị xoá`; `SYS_UDT_DT = ETL_DATE`

**Target:** `SILVER.LOAN_AR`
**Recon:** Agg sum(`LIMIT_AMT`/`ORIG_AMT_FCY`) & Minus test.
**File SQL:** `sql/silver_temp2/loan_ar.sql`

---

## 6. PHASE 3 — Task 11: `LOAN_AR_PRFL` (Silver Temp2 — Bank-wised, SCD Type 2)

**2 nhánh UNION ALL** tương tự LOAN_AR (nhánh chính từ `LD_LOANS_AND_DEPOSITS`, nhánh khởi tạo từ `LD_LOANS_AND_DEPOSITS_HIS`).

**Nhánh chính:**
```
BRONZE.T24CORE_LD_LOANS_AND_DEPOSITS  a
  LEFT JOIN BRONZE.EBANKING_COL_UDF_VALUE  b
    ON ((b.COL_REF = a.ACC_FCC AND b.FUNCTION_CODE='CONTRACT_INFOR' AND a.RECID LIKE 'LD%')
        OR (b.COL_REF = a.FCC_LIMIT AND b.FUNCTION_CODE='LIMIT_INFOR'))
   AND b.UDF_ID = 'NHANNO_BATBUOC'
  LEFT JOIN BRONZE.FLEXBO_PGB_CONTRACT_UDF_MAP  c   ON c.CONTRACT_REF_NO=a.ACC_FCC AND c.FIELD_NAME='SO LAN CO CAU'
  LEFT JOIN BRONZE.FLEXBO_PGB_CONTRACT_UDF_MAP  c1  ON c1.CONTRACT_REF_NO=a.ACC_FCC AND c1.FIELD_NAME='LOAI CHUNG KHOAN'
  LEFT JOIN BRONZE.FLEXBO_PGB_LOS_CONTRACT_FIELDS_TDATE  d
    ON d.CONTRACT_REF_NO=a.RECID AND d.MOV_DATE=TO_DATE(COALESCE(a.ORIG_VAL_DATE,a.VALUE_DATE),'YYYYMMDD')
  LEFT JOIN BRONZE.FLEXBO_PGBLD_CONTRACT_UDFIELD_HIST  e  ON e.CONTRACT_REF_NO=a.ACC_FCC   -- tránh dup dữ liệu, KHÔNG filter MOV_DATE
  LEFT JOIN BRONZE.T24CORE_MB_MG_SAVING_MULTI  f  ON f.LD_ID=a.RECID
  LEFT JOIN BRONZE.FLEXBO_PGBLD_RT_CONTRACT_UDFIELD_HIST  g  ON g.CONTRACT_REF_NO=a.ACC_FCC   -- tránh dup, KHÔNG filter MOV_DATE
  LEFT JOIN BRONZE.SOURCE_SAOKE_MVMT  h  ON h.TRANS_REF=a.RECID AND h.SBV_CODE LIKE '39%'      -- DISTINCT TRANS_REF, SBV_CODE trước join
  LEFT JOIN BRONZE.T24CORE_CUSTOMER  i  ON i.RECID=a.CUSTOMER_ID
```

**Nhánh khởi tạo lịch sử:** từ `LD_LOANS_AND_DEPOSITS_HIS` (alias `a`), điều kiện lọc bản ghi đóng tương tự Phase 2, join lại `b,c,c1,d,e` (dùng `SUBSTR` khi cần).

**Transform / Business rule chính:**
- `AR_ID = fn_hash("T24_LD_LOANS_AND_DEPOSITS", RECID)`
- `INT_LQD_AR_CODE/ID = INT_LIQ_ACCT`; `PNP_LQD_AR_CODE/ID = PRIN_LIQ_ACCT`
- `AR_LCS_TP_CODE = STATUS`; `OU_CODE = CO_CODE`
- `MAND_REPYMT = b.UDF_VALUE`
- `NBR_OF_DBT_RSTC = NVL(d.SO_LAN_CO_CAU, c.FIELD_VAL)` — **ưu tiên `d` (TDATE) trước `c` (UDF_MAP)**
- `SCR_TP = c1.FIELD_VAL`
- `CAR_AP_CODE = d.CAR_APPLICATION_CODE`; `LOS_CTR_CODE = d.LOS_CONTRACT_CODE`; `LOS_CTR_ID = d.LOS_CONTRACT_ID`
- `DBT_WVR_F = d.CO_CAU_NO_MIEN_GIAM_LAI`
- `AR_PPS_BY_IDY = d.MDVAY_NKT`; `AR_PPS_BY_PD = d.MDVAY_SP`
- `DSBR_PRPSL_OFCR = d.CAN_BO_DE_XUAT_GN`; `DSBR_PRPSL_MGR = d.TT_PP_DE_XUAT_GN`
- `MGT_OFCR = d.CBTD_QL_KHOAN_VAY`; `MGT_MGR = d.TP_PP_QL_KHOAN_VAY`; `APRV_AHR = d.CAP_PHE_DUYET_GNTL`
- `CHKER_OFCR = AUTHORISER`; `MAKER_OFCR = INPUTTER`; `REFRRER_OFCR = MB_RM_BANCHEO`
- `PROM_PRGM = CASE WHEN e.CT_KHUYEN_MAI_CHO_VAY IS NULL AND i.KHOI='INDIV' THEN f.MB_LD_TYPE ELSE e.CT_KHUYEN_MAI_CHO_VAY END`
- `PROM_PRGM_2 = CASE WHEN e.CTKM2 IS NULL AND i.KHOI<>'INDIV' THEN f.MB_LD_TYPE END`
- `DBT_RSTC_TP_CODE = g.CO_CAU_NO`; `COVID19_RSTC_COMPL_DT = g.NGAY_HET_COVID19`
- `GL_ITM_CODE/GL_ITM_ID = h.SBV_CODE`
- `FNC_ST_CODE = '1'` (literal mặc định)
- `SYS_EFF_DT/SYS_EXP_DT/SYS_UDT_DT`: chuẩn SCD2 theo `AR_ID`

**Target:** `SILVER.LOAN_AR_PRFL`
**Recon:** Row count & duplicate `AR_ID`.
**File SQL:** `sql/silver_temp2/loan_ar_prfl.sql`

---

## 7. PHASE 4 — Task 9: `INTF_LOAN_AR` (Join 8 bảng Silver — "Interface" xuất)

> Đây là bước **cuối cùng**, chỉ chạy khi Phase 1 (6 bảng vệ tinh) + Phase 2 (`LOAN_AR`) + Phase 3 (`LOAN_AR_PRFL`) đã hoàn tất.

```
SILVER.AR_BAL  a
  JOIN       SILVER.LOAN_AR       b   ON a.AR_ID = b.AR_ID
  JOIN       SILVER.LOAN_AR_PRFL  c   ON a.AR_ID = c.AR_ID
  JOIN       SILVER.AR_RATE_HIST  d   ON a.AR_ID = d.AR_ID
  LEFT JOIN  SILVER.AR_DLQ_SMY    e   ON a.AR_ID = e.AR_ID
  LEFT JOIN (
      SELECT ORIG_AR_ID,
             SUM(${ETL_DATE} - PNP_PAST_DUE_DT) + 1                       AS SO_NGAY_QH_GOC,
             SUM(${ETL_DATE} - INT_PAST_DUE_DT) + 1                       AS SO_NGAY_QH_LAI,
             GREATEST(NVL(SUM(${ETL_DATE}-PNP_PAST_DUE_DT),0),
                       NVL(SUM(${ETL_DATE}-INT_PAST_DUE_DT),0)) + 1        AS SONGAY_QHAN_CAONHAT,
             SUM(NVL(ADDITION_DYS_IN_ARS,0))                              AS SNQH_CHUYENDOI
      FROM SILVER.AR_DLQ_SMY GROUP BY ORIG_AR_ID
  ) e1  ON e1.ORIG_AR_ID = a.AR_ID
  LEFT JOIN  SILVER.EXG_RATE_HIST f   ON b.CCY_CODE = f.FRST_CCY_CODE
  LEFT JOIN  SILVER.OU            i   ON a.OU_CODE = i.OU_CODE
  LEFT JOIN (
      SELECT ORIG_AR_ID,
             SUM(TOT_ACR_INT_AMT_FCY) TOT_ACR_INT_AMT_FCY,
             SUM(TOT_INT_PAID_AMT_FCY) TOT_INT_PAID_AMT_FCY,
             SUM(TOT_INT_DUE_AMT_FCY) TOT_INT_DUE_AMT_FCY,
             SUM(TOT_INT_ODUE_AMT_FCY) TOT_INT_ODUE_AMT_FCY
      FROM SILVER.AST_AR_INT_SMY GROUP BY ORIG_AR_ID
  ) k  ON a.AR_ID = k.ORIG_AR_ID
```

**Transform chính (map sang `INTF_LOAN_AR`):**
- `CDR_DT = current date`
- `AR_CODE = a.AR_CODE`; `EFF_DT = b.EFF_DT`; `PD_CGY_CODE = a.PD_CGY_CODE`; `PPR_CTR_NBR = b.PPR_CTR_NBR`
- `OU_CODE = c.OU_CODE`; `SUB_PD_CODE = a.SUB_PD_CODE`; `CST_CODE = a.CST_CODE`; `AR_LCS_TP_CODE = c.AR_LCS_TP_CODE`
- `MUD_CODE = a.MUD_CODE`; `FNC_ST_CODE = c.FNC_ST_CODE`; `CCY_CODE = b.CCY_CODE`; `MAT_DT = b.MAT_DT`
- `VAL_DT = NVL(b.FCC_VAL_DT, b.EFF_DT)`; `AR_TERM_TP_CODE = a.AR_TERM_TP_CODE`; `LMT_CODE = b.LMT_CODE`
- `INT_LQD_AR_CODE = c.INT_LQD_AR_CODE`; `FCC_AR_CODE = b.FCC_AR_CODE`
- `PNY_RATE = d.PNY_RATE`; `ODUE_INT_RATE = d.ODUE_INT_RATE`; `MRGN_RATE = d.MRGN_RATE`
- `TXN_DT = a.CDR_DT`
- `ODUE_IN_AMT = k.TOT_ACR_INT_AMT_FCY`
- `PD_CODE = b.PD_CODE` (2 dòng mapping trong sheet gốc, cả `SAN_PHAM_CHO_VAY_KHDN` và `SAN_PHAM_CHO_VAY_KHCN` đều trỏ về `b.PD_CODE` — không có logic khác biệt bổ sung, giữ nguyên `b.PD_CODE`)
- `DSBR_PRPSL_OFCR = c.DSBR_PRPSL_OFCR`; `DSBR_PRPSL_MGR = c.DSBR_PRPSL_MGR`; `MGT_OFCR = c.MGT_OFCR`; `MGT_MGR = c.MGT_MGR`
- `APRV_AHR = c.APRV_AHR`; `DBT_WVR_F = c.DBT_WVR_F`; `NBR_OF_DBT_RSTC = c.NBR_OF_DBT_RSTC`
- `AR_PPS_BY_PD = b.AR_PPS_TP_CODE`
- `DYS_IN_PNP_ARS = e1.SO_NGAY_QH_GOC`; `DYS_IN_INT_ARS = e1.SO_NGAY_QH_LAI`
- Các cột literal cố định (theo sheet gốc, giữ nguyên vì chưa có nguồn xác định): `APPROVE_AMOUNT='0'`, `DRAWDOWN_ACCOUNT=NULL`, `INT_KEY=NULL`, `INT_SPRD=NULL`, `AMOUNT_CUR='0'`, `INT_TP=NULL`, `INT_BSS=NULL`, `INT_FIX=NULL`, `RATE=NULL`, `ODUE_PR_AMT='0'`, `ODUE_PE_AMT='0'`, `ODUE_PS_AMT='0'`

**Target:** `PG_GOLD.INTF_LOAN_AR` *(theo COMMENT trong sheet, mặc dù `Phan_Cong_Task` gọi tầng là "Silver Temp1" — cần xác nhận schema thật: `PG_GOLD` hay `PG_SILVER`)*
**Recon:** So sánh row count T24 gốc vs bảng đích.
**Dependency Airflow:** `wait_for = [ar_bal, ar_rate_hist, ar_dlq_smy, ou, exg_rate, ast_ar_int_smy, loan_ar, loan_ar_prfl]` (toàn bộ Phase 1–3 trong DAG này).
**File SQL:** `sql/silver_temp1/intf_loan_ar.sql` *(giữ tên file theo `Phan_Cong_Task` gốc dù layer thực tế nên xem lại)*

---

## 8. PHASE 5 — Task 12: `DIM_LOAN_AR` (Gold / Data Mart — SCD Type 2)

```
SILVER.LOAN_AR       a
  LEFT JOIN SILVER.LOAN_AR_PRFL  b   ON a.AR_ID = b.AR_ID
```

**Transform:**
- `DIM_KEY = TO_CHAR(ETL_CHANGE_DATE,'YYYYMMDD') || AR_ID`
- Map 1:1: `AR_ID`, `AR_CODE`, `FCC_AR_CODE`, `PPR_CTR_NBR`, `PD_CGY_CODE`, `AR_PPS_GRP_CODE`, `AR_PPS_TP_CODE`, `ORIG_AMT_FCY`, `TERM_CODE`, `VAL_DT`, `EFF_DT`, `MAT_DT` (từ `a`)
- Map 1:1: `AR_LCS_TP_CODE` (từ `b`), `PROM_PRGM→PROM_CODE`, `PROM_PRGM_2→PROM_CODE_2` (từ `b`)
- `SYS_EFF_DT`, `SYS_EXP_DT`: SCD2 tracking

**Target:** `PG_GOLD.DIM_LOAN_AR`
**Recon:** SCD2 validity overlap check.
**Dependency:** chỉ cần Phase 2 + Phase 3 (không phụ thuộc Phase 1/4).
**File SQL:** `sql/silver_temp2/dim_loan_ar.sql`

---

## 9. SƠ ĐỒ DEPENDENCY TỔNG THỂ (DAG logic)

```
[21 Bronze Load Tasks — song song, PHASE 0]
        │
        ├──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
        ▼              ▼              ▼              ▼              ▼              ▼
   silver_ar_bal  silver_ar_rate  silver_ar_dlq   silver_ou   silver_exg_rate  silver_ast_int
     (Phase 1)      (Phase 1)      (Phase 1)      (Phase 1)      (Phase 1)      (Phase 1)
        │              │              │              │              │              │
        │         ┌────┴────┐         │              │              │              │
        ▼         ▼         ▼         ▼              ▼              ▼              ▼
   ┌─────────────────────── silver_loan_ar (Phase 2, Task10) ───────────────────────┐
   │                    silver_loan_ar_prfl (Phase 3, Task11)                       │
   └────────────────────────────────┬──────────────────────────────────────────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    ▼                                  ▼
        intf_loan_ar (Phase 4, Task9)        dim_loan_ar (Phase 5, Task12)
     [chờ TOÀN BỘ Phase 1 + loan_ar + loan_ar_prfl]   [chỉ chờ loan_ar + loan_ar_prfl]
```

---

## 10. CHECKLIST RECONCILIATION TỔNG HỢP

| Task | Bảng đích | Phương pháp Recon |
|---|---|---|
| Phase 0 (x21) | Bronze tables | Row count Source vs Bronze theo batch |
| Phase 1.1 | AR_BAL | Agg sum(RCVB_AMT_LCY, CLS_BAL_LCY) & Minus test |
| Phase 1.2 | AR_RATE_HIST | Row count rate changes |
| Phase 1.3 | AR_DLQ_SMY | Checksum overdue balances per DPD bucket |
| Phase 1.4 | OU | Minus test OU master |
| Phase 1.5 | EXG_RATE(_HIST) | Minus test Currency vs Exchange Rate |
| Phase 1.6 | AST_AR_INT_SMY | Agg sum(TOT_ACR_INT_AMT_FCY) |
| Phase 2 | LOAN_AR | Agg sum(LIMIT_AMT) & Minus test |
| Phase 3 | LOAN_AR_PRFL | Row count & duplicate AR_ID |
| Phase 4 | INTF_LOAN_AR | So sánh row count T24 gốc vs bảng đích |
| Phase 5 | DIM_LOAN_AR | SCD2 validity overlap check |

---

## 11. DANH SÁCH FILE SQL / AIRFLOW CẦN TẠO (đề xuất cấu trúc thư mục)

```
dags/
  dag_track2_loan_standalone.py        # 1 DAG duy nhất, TaskGroup theo Phase 0-5

sql/
  bronze/
    t24core_ld_loans_and_deposits.sql
    t24core_ld_loans_and_deposits_his.sql
    flexbo_pgb_ldtb_contract_master.sql
    flexbo_pgb_los_contract_fields_tdate.sql
    los_app_loan_disbursement.sql
    los_app_facility.sql
    los_app_product.sql
    t24core_customer.sql
    ebanking_col_udf_value.sql
    flexbo_pgb_contract_udf_map.sql
    flexbo_pgbld_contract_udfield_hist.sql
    t24core_mb_mg_saving_multi.sql
    flexbo_pgbld_rt_contract_udfield_hist.sql
    source_saoke_mvmt.sql
    source_saoke_crb.sql
    t24core_stmt_entry.sql
    t24core_pd_payment_due_his_mv.sql
    t24core_pd_payment_due.sql
    t24core_company.sql
    t24core_currency.sql
    t24core_currency_his.sql
  silver_satellite/                     # PHASE 1 - build lại, KHÔNG kế thừa
    ar_bal.sql
    ar_rate_hist.sql
    ar_dlq_smy.sql
    ou.sql
    exg_rate.sql
    ast_ar_int_smy.sql
  silver_temp2/
    loan_ar.sql                         # Task 10
    loan_ar_prfl.sql                    # Task 11
    dim_loan_ar.sql                     # Task 12
  silver_temp1/
    intf_loan_ar.sql                    # Task 9
```

---

## 12. VIỆC CẦN AGENT XÁC NHẬN TRƯỚC KHI CODE (Open Items)

1. Tên schema thật của `INTF_LOAN_AR` trong DB (`PG_GOLD` theo COMMENT trong sheet mapping, hay `PG_SILVER` theo label trong `Phan_Cong_Task`?).
2. Xác nhận `EXG_RATE_HIST` có phải là bảng riêng biệt (full lịch sử) khác với `EXG_RATE` (snapshot mới nhất) hay là cùng 1 bảng bị đặt tên khác trong 2 sheet.
3. Xác nhận kiểu dữ liệu/độ dài chính xác từng cột theo DDL thật trong DB (sheet Excel có một số ô để trống `Kiểu dữ liệu`/`Độ dài`, ví dụ ở `LOAN_AR_PRFL.INT_LQD_AR_ID`, `LOAN_AR_PRFL.GL_ITM_ID`).
4. Xác nhận literal mặc định trong `INTF_LOAN_AR` (`APPROVE_AMOUNT='0'`, các cột `NULL`...) có đúng là placeholder chờ nguồn hay là giá trị nghiệp vụ cố định thật sự.
5. Xác nhận `fn_hash()` là UDF có sẵn trong DB target (Spark SQL) hay cần Fresher 2 tự định nghĩa UDF này trong pipeline.
