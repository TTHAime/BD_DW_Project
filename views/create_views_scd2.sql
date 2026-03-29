/* ======================================================================
   DASHBOARD VIEWS — SCD TYPE 2 VERSION
   Base views + 5 Report-specific views

   Run after: etl_load_dw_scd2_fixed.sql

   ====================================================================== */

/* ==========================================================
   BASE VIEW: ORDER_LINE_FLAT
   Fact + all Dims joined ผ่าน surrogate key
   ทุก column ใช้จาก SCD version ที่ถูก pin ไว้ใน FACT แล้ว
   ========================================================== */
CREATE OR REPLACE VIEW ORDER_LINE_FLAT AS
SELECT
  /* ---------- Date dim ---------- */
  fo.ORDER_DATE_KEY,
  dd.DATE_VALUE,
  dd.DAY_OF_MONTH,
  dd.DAY_NAME,
  dd.DAY_OF_WEEK_NUM,
  dd.WEEK_OF_YEAR,
  dd.MONTH_NUM,
  dd.MONTH_NAME,
  dd.QUARTER_NUM,
  dd.YEAR_NUM,
  dd.IS_WEEKEND,

  /* ---------- Category ---------- */
  cat.CAT_ID,
  cat.NAME                                   AS CAT_NAME,

  /* ---------- Line detail ---------- */
  fol.COMMENT_TEXT,
  pdim.DESCRIPTION,
  fol.LINE_AMOUNT,
  fol.DISCOUNT                               AS LINE_DISCOUNT,
  (NVL(fol.QTY,0) * NVL(fol.UNIT_PRICE,0))  AS LIST_PRICE,

  /* ---------- Order ---------- */
  fol.ORD_ID,
  fo.ORDER_DATE,
  fo.PAY_STAT_ID,
  pay.NAME                                   AS PAY_STAT_NAME,

  /* ---------- Product (version ณ order date) ---------- */
  pdim.DISCOUNT                              AS PRD_DISCOUNT,
  pdim.PRD_ID,
  fol.PRD_KEY,
  pdim.PRD_NAME,
  pdim.PRICE                                 AS PRD_PRICE,
  pdim.STOCK                                 AS PRD_STOCK,
  pdim.PRD_TYPE_ID,
  ptype.NAME                                 AS PRD_TYPE_NAME,
  pdim.EFFECTIVE_DATE                        AS PRD_EFFECTIVE_DATE,
  pdim.SCD_END_DATE                          AS PRD_SCD_END_DATE,
  pdim.IS_CURRENT                            AS PRD_IS_CURRENT,

  /* ---------- Line measures ---------- */
  fol.QTY,
  fol.RATING,
  fol.SEQ,
  fol.UNIT_PRICE,

  /* ---------- Shop (version ณ order date) ---------- */
  sdim.RATING_AVG,
  sdim.SHOP_ID,
  fol.SHOP_KEY,
  sdim.SHOP_NAME,
  sdim.EFFECTIVE_DATE                        AS SHOP_EFFECTIVE_DATE,
  sdim.SCD_END_DATE                          AS SHOP_SCD_END_DATE,
  sdim.IS_CURRENT                            AS SHOP_IS_CURRENT,

  /* ---------- Shipping status ---------- */
  fol.SHP_STAT_ID,
  shp.NAME                                   AS SHP_STAT_NAME,

  /* ---------- Order totals (header) ---------- */
  fo.TOTAL_AMOUNT,
  fo.TOTAL_DISCOUNT,

  /* ---------- User ---------- */
  fo.USR_ID

FROM DW_FACT_ORDER_LINE fol
JOIN DW_FACT_ORDER fo           ON fo.ORD_ID        = fol.ORD_ID
LEFT JOIN DW_DIM_DATE dd        ON dd.DATE_KEY       = fo.ORDER_DATE_KEY
LEFT JOIN DW_DIM_PRODUCT pdim   ON pdim.PRD_KEY      = fol.PRD_KEY
LEFT JOIN DW_DIM_SHOP sdim      ON sdim.SHOP_KEY     = fol.SHOP_KEY
LEFT JOIN DW_DIM_CATEGORY cat   ON cat.CAT_ID        = pdim.CAT_ID
LEFT JOIN DW_DIM_PRD_TYPE ptype ON ptype.PRD_TYPE_ID = pdim.PRD_TYPE_ID
LEFT JOIN DW_DIM_PAY_STAT pay   ON pay.PAY_STAT_ID   = fo.PAY_STAT_ID
LEFT JOIN DW_DIM_SHP_STAT shp   ON shp.SHP_STAT_ID   = fol.SHP_STAT_ID;
/

/* ==========================================================
   BASE VIEW: VW_DASH_CAMPAIGN_PRODUCT
   Bridge + Campaign + Product (current version เท่านั้น)

   FIX: JOIN Bridge ด้วย surrogate key (CMP_KEY, PRD_KEY)
   ที่ถูก pin ตอน ETL → ได้ version ที่ตรงกับ source data
   ========================================================== */
CREATE OR REPLACE VIEW VW_DASH_CAMPAIGN_PRODUCT AS
SELECT
  c.CMP_KEY,
  c.CMP_ID,
  c.CMP_NAME,
  c.DISCOUNT        AS CMP_DISCOUNT,
  c.CMP_START_DATE,                 -- business: วันเริ่มแคมเปญ
  c.CMP_END_DATE,                   -- business: วันสิ้นสุดแคมเปญ
  c.PERIOD_ID,

  p.PRD_KEY,
  p.PRD_ID,
  p.PRD_NAME,
  p.PRICE           AS PRD_PRICE,
  p.DISCOUNT        AS PRD_DISCOUNT,
  p.CAT_ID,
  p.PRD_TYPE_ID,
  p.SHOP_ID

FROM DW_BRIDGE_CAMPAIGN_PRODUCT b
JOIN DW_DIM_CAMPAIGN c ON c.CMP_KEY = b.CMP_KEY   -- surrogate key join
JOIN DW_DIM_PRODUCT  p ON p.PRD_KEY = b.PRD_KEY;  -- surrogate key join
/

/* ==========================================================
   CONVENIENCE: Current dimension views
   ========================================================== */
CREATE OR REPLACE VIEW VW_DIM_PRODUCT_CURRENT AS
SELECT * FROM DW_DIM_PRODUCT WHERE IS_CURRENT = 'Y';
/

CREATE OR REPLACE VIEW VW_DIM_SHOP_CURRENT AS
SELECT * FROM DW_DIM_SHOP WHERE IS_CURRENT = 'Y';
/

CREATE OR REPLACE VIEW VW_DIM_CAMPAIGN_CURRENT AS
SELECT * FROM DW_DIM_CAMPAIGN WHERE IS_CURRENT = 'Y';
/

/* ======================================================================
   REPORT VIEWS (1 per report)
   ====================================================================== */

/* ==========================================================
   REPORT 1: VW_RPT_SALES_TOP5
   ยอดขาย Top 5 สินค้า แยกตามหมวด ร้านค้า ช่วงเวลา

   Bus Matrix dims: DATE, PRODUCT, CATEGORY, PRD_TYPE, SHOP
   Bus Matrix measures:
     SUM(LINE_AMOUNT)  → TOTAL_REVENUE
     SUM(QTY)          → TOTAL_QTY
     COUNT(ORD_ID)     → ORDER_COUNT
     AVG(RATING)       → AVG_RATING

   Dashboard: Bar Chart Top 5, Donut by Category, Matrix Product x Month
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SALES_TOP5 AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
  DAY_OF_MONTH,
  DAY_NAME,
  IS_WEEKEND,

  PRD_ID,
  PRD_KEY,
  PRD_NAME,
  PRD_TYPE_NAME,
  CAT_ID,
  CAT_NAME,

  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,

  SUM(LINE_AMOUNT)            AS TOTAL_REVENUE,
  SUM(QTY)                    AS TOTAL_QTY,
  COUNT(DISTINCT ORD_ID)      AS ORDER_COUNT,
  ROUND(AVG(RATING), 2)       AS AVG_RATING,
  COUNT(RATING)               AS RATING_COUNT

FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME,
  DAY_OF_MONTH, DAY_NAME, IS_WEEKEND,
  PRD_ID, PRD_KEY, PRD_NAME, PRD_TYPE_NAME,
  CAT_ID, CAT_NAME,
  SHOP_KEY, SHOP_ID, SHOP_NAME;
/

/* ==========================================================
   REPORT 2: VW_RPT_CUSTOMER_BEHAVIOR
   พฤติกรรมการซื้อของลูกค้า แยกตามหมวด สถานะชำระ
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_CUSTOMER_BEHAVIOR AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
  DAY_NAME,
  IS_WEEKEND,

  USR_ID,

  CAT_ID,
  CAT_NAME,

  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,

  PAY_STAT_ID,
  PAY_STAT_NAME,

  COUNT(DISTINCT ORD_ID)                                AS ORDER_COUNT,
  SUM(LINE_AMOUNT)                                      AS TOTAL_REVENUE,
  SUM(QTY)                                              AS TOTAL_QTY,
  ROUND(
    CASE WHEN COUNT(DISTINCT ORD_ID) > 0
         THEN SUM(LINE_AMOUNT) / COUNT(DISTINCT ORD_ID)
         ELSE 0
    END, 2
  )                                                     AS AVG_ORDER_VALUE

FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME,
  DAY_NAME, IS_WEEKEND,
  USR_ID,
  CAT_ID, CAT_NAME,
  SHOP_KEY, SHOP_ID, SHOP_NAME,
  PAY_STAT_ID, PAY_STAT_NAME;
/

/* ==========================================================
   REPORT 3: VW_RPT_CAMPAIGN_EFFECTIVENESS
   ประสิทธิภาพแคมเปญ — เทียบยอดขายสินค้าใน/นอกแคมเปญ
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_CAMPAIGN_EFFECTIVENESS AS
SELECT
  sub.YEAR_NUM,
  sub.QUARTER_NUM,
  sub.MONTH_NUM,
  sub.MONTH_NAME,
  sub.DAY_NAME,
  sub.IS_WEEKEND,
  sub.ORDER_DATE,

  sub.PRD_ID,
  sub.PRD_KEY,
  sub.PRD_NAME,
  sub.CAT_ID,
  sub.CAT_NAME,

  sub.SHOP_KEY,
  sub.SHOP_ID,
  sub.SHOP_NAME,

  sub.CMP_KEY,
  sub.CMP_ID,
  sub.CMP_NAME,
  sub.CMP_DISCOUNT,
  sub.CMP_START_DATE,
  sub.CMP_END_DATE,
  sub.CAMPAIGN_FLAG,

  sub.ORD_ID,
  sub.SEQ,
  sub.QTY,
  sub.UNIT_PRICE,
  sub.LINE_DISCOUNT,
  sub.LINE_AMOUNT,
  sub.RATING

FROM (
  SELECT
    olf.YEAR_NUM,
    olf.QUARTER_NUM,
    olf.MONTH_NUM,
    olf.MONTH_NAME,
    olf.DAY_NAME,
    olf.IS_WEEKEND,
    olf.ORDER_DATE,

    olf.PRD_ID,
    olf.PRD_KEY,
    olf.PRD_NAME,
    olf.CAT_ID,
    olf.CAT_NAME,

    olf.SHOP_KEY,
    olf.SHOP_ID,
    olf.SHOP_NAME,

    cp.CMP_KEY,
    cp.CMP_ID,
    cp.CMP_NAME,
    cp.CMP_DISCOUNT,
    cp.CMP_START_DATE,
    cp.CMP_END_DATE,

    CASE
      WHEN cp.CMP_ID IS NOT NULL THEN 'In Campaign'
      ELSE 'Not in Campaign'
    END                           AS CAMPAIGN_FLAG,

    olf.ORD_ID,
    olf.SEQ,
    olf.QTY,
    olf.UNIT_PRICE,
    olf.LINE_DISCOUNT,
    olf.LINE_AMOUNT,
    olf.RATING,

    -- FIX: เพิ่ม CMP_KEY เป็น tie-break เพื่อ deterministic
    ROW_NUMBER() OVER (
      PARTITION BY olf.ORD_ID, olf.SEQ
      ORDER BY cp.CMP_DISCOUNT DESC NULLS LAST, cp.CMP_KEY ASC NULLS LAST
    ) AS RN

  FROM ORDER_LINE_FLAT olf
  LEFT JOIN VW_DASH_CAMPAIGN_PRODUCT cp
    ON  cp.PRD_ID = olf.PRD_ID
    AND olf.ORDER_DATE BETWEEN cp.CMP_START_DATE AND cp.CMP_END_DATE
) sub
WHERE sub.RN = 1;
/

/* ==========================================================
   REPORT 4: VW_RPT_SHOP_PERFORMANCE
   ประสิทธิภาพร้านค้า — ออร์เดอร์ แยกตามร้าน สถานะจัดส่ง
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SHOP_PERFORMANCE AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
  WEEK_OF_YEAR,
  DAY_OF_MONTH,
  DAY_NAME,
  IS_WEEKEND,

  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
  RATING_AVG                   AS SHOP_RATING_AVG,

  SHP_STAT_ID,
  SHP_STAT_NAME,

  COUNT(DISTINCT ORD_ID)       AS ORDER_COUNT,
  SUM(QTY)                     AS TOTAL_QTY,
  SUM(LINE_AMOUNT)             AS TOTAL_REVENUE,
  ROUND(AVG(RATING), 2)        AS AVG_LINE_RATING,
  COUNT(RATING)                AS RATING_COUNT

FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME,
  WEEK_OF_YEAR, DAY_OF_MONTH, DAY_NAME, IS_WEEKEND,
  SHOP_KEY, SHOP_ID, SHOP_NAME, RATING_AVG,
  SHP_STAT_ID, SHP_STAT_NAME;
/

/* ==========================================================
   REPORT 5: VW_RPT_SATISFACTION
   ความพึงพอใจ — RATING + COMMENT แยกตามร้าน สินค้า สถานะ

   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SATISFACTION AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
  DAY_NAME,
  IS_WEEKEND,
  ORDER_DATE,

  PRD_ID,
  PRD_KEY,
  PRD_NAME,
  CAT_NAME,

  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
  RATING_AVG                    AS SHOP_RATING_AVG,

  PAY_STAT_ID,
  PAY_STAT_NAME,
  SHP_STAT_ID,
  SHP_STAT_NAME,

  USR_ID,
  ORD_ID,
  SEQ,
  RATING,
  COMMENT_TEXT,

  CASE WHEN RATING IS NOT NULL     THEN 1 ELSE 0 END  AS HAS_RATING,
  CASE WHEN COMMENT_TEXT IS NOT NULL THEN 1 ELSE 0 END AS HAS_COMMENT,

  CASE
    WHEN RATING >= 4 THEN 'Good'
    WHEN RATING  = 3 THEN 'Mid'
    WHEN RATING <= 2 THEN 'Low'
    ELSE 'No Rating'
  END                           AS RATING_GROUP

FROM ORDER_LINE_FLAT;
/
