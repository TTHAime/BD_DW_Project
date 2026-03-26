/* ======================================================================
   DASHBOARD VIEWS — SCD TYPE 2 VERSION
   Base views + 5 Report-specific views
   
   Run after: etl_load_dw_scd2.sql
   ====================================================================== */
 
/* ==========================================================
   BASE VIEW: ORDER_LINE_FLAT
   Fact + all Dims joined via surrogate keys
   Used as foundation for all 5 report views
   ========================================================== */
CREATE OR REPLACE VIEW ORDER_LINE_FLAT AS
SELECT
  fo.ORDER_DATE_KEY,
  dd.DATE_VALUE,
  dd.DAY_OF_MONTH,
  dd.DAY_NAME,
  dd.WEEK_OF_YEAR,
  dd.MONTH_NUM,
  dd.MONTH_NAME,
  dd.QUARTER_NUM,
  dd.YEAR_NUM,
  dd.IS_WEEKEND,
 
  cat.CAT_ID,
  cat.NAME                                    AS CAT_NAME,
 
  fol.COMMENT_TEXT,
  pdim.DESCRIPTION,
  fol.LINE_AMOUNT,
  fol.DISCOUNT                                AS LINE_DISCOUNT,
  (NVL(fol.QTY,0) * NVL(fol.UNIT_PRICE,0))   AS LIST_PRICE,
 
  fol.ORD_ID,
  fo.ORDER_DATE,
  fo.PAY_STAT_ID,
  pay.NAME                                    AS PAY_STAT_NAME,
 
  pdim.DISCOUNT                               AS PRD_DISCOUNT,
  pdim.PRD_ID,
  fol.PRD_KEY,
  pdim.PRD_NAME,
  pdim.PRICE                                  AS PRD_PRICE,
  pdim.STOCK                                  AS PRD_STOCK,
  pdim.PRD_TYPE_ID,
  ptype.NAME                                  AS PRD_TYPE_NAME,
 
  pdim.EFFECTIVE_DATE                         AS PRD_EFFECTIVE_DATE,
  pdim.END_DATE                               AS PRD_END_DATE,
  pdim.IS_CURRENT                             AS PRD_IS_CURRENT,
 
  fol.QTY,
  fol.RATING,
 
  sdim.RATING_AVG,
  fol.SEQ,
  sdim.SHOP_ID,
  fol.SHOP_KEY,
  sdim.SHOP_NAME,
 
  sdim.EFFECTIVE_DATE                         AS SHOP_EFFECTIVE_DATE,
  sdim.END_DATE                               AS SHOP_END_DATE,
  sdim.IS_CURRENT                             AS SHOP_IS_CURRENT,
 
  fol.SHP_STAT_ID,
  shp.NAME                                    AS SHP_STAT_NAME,
 
  fo.TOTAL_AMOUNT,
  fo.TOTAL_DISCOUNT,
 
  fol.UNIT_PRICE,
  fo.USR_ID
 
FROM DW_FACT_ORDER_LINE fol
JOIN DW_FACT_ORDER fo       ON fo.ORD_ID      = fol.ORD_ID
LEFT JOIN DW_DIM_DATE dd    ON dd.DATE_KEY    = fo.ORDER_DATE_KEY
LEFT JOIN DW_DIM_PRODUCT pdim ON pdim.PRD_KEY = fol.PRD_KEY
LEFT JOIN DW_DIM_SHOP sdim  ON sdim.SHOP_KEY  = fol.SHOP_KEY
LEFT JOIN DW_DIM_CATEGORY cat ON cat.CAT_ID   = pdim.CAT_ID
LEFT JOIN DW_DIM_PRD_TYPE ptype ON ptype.PRD_TYPE_ID = pdim.PRD_TYPE_ID
LEFT JOIN DW_DIM_PAY_STAT pay ON pay.PAY_STAT_ID    = fo.PAY_STAT_ID
LEFT JOIN DW_DIM_SHP_STAT shp ON shp.SHP_STAT_ID    = fol.SHP_STAT_ID;
/
 
/* ==========================================================
   BASE VIEW: VW_DASH_CAMPAIGN_PRODUCT
   Bridge + Campaign + Product (current versions)
   ========================================================== */
CREATE OR REPLACE VIEW VW_DASH_CAMPAIGN_PRODUCT AS
SELECT
  c.CMP_KEY,
  c.CMP_ID,
  c.CMP_NAME,
  c.DISCOUNT        AS CMP_DISCOUNT,
  c.START_DATE       AS CMP_START_DATE,
  c.END_DATE         AS CMP_END_DATE,
  c.PERIOD_ID,
 
  p.PRD_KEY,
  p.PRD_ID,
  p.PRD_NAME,
  p.PRICE            AS PRD_PRICE,
  p.DISCOUNT         AS PRD_DISCOUNT,
  p.CAT_ID,
  p.PRD_TYPE_ID,
  p.SHOP_ID
 
FROM DW_BRIDGE_CAMPAIGN_PRODUCT b
JOIN DW_DIM_CAMPAIGN c ON c.CMP_ID = b.CMP_ID AND c.IS_CURRENT = 'Y'
JOIN DW_DIM_PRODUCT p  ON p.PRD_ID = b.PRD_ID AND p.IS_CURRENT = 'Y';
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
 
 
/* ======================================================================
   REPORT VIEWS — 1 view ต่อ 1 รายงานเชิงบริหาร
   ====================================================================== */
 
/* ==========================================================
   REPORT 1: VW_RPT_SALES_TOP5
   ยอดขาย Top 5 สินค้า แยกตามหมวด ร้านค้า ช่วงเวลา
   
   Dims: DATE, PRODUCT, CATEGORY, PRD_TYPE, SHOP
   Measures: SUM(LINE_AMOUNT), SUM(QTY), COUNT(ORD_ID), AVG(RATING)
   
   Dashboard: Bar Chart Top 5, Donut by Category, Matrix Product x Month
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SALES_TOP5 AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
 
  PRD_ID,
  PRD_KEY,
  PRD_NAME,
  PRD_TYPE_NAME,
  CAT_ID,
  CAT_NAME,
 
  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
 
  SUM(LINE_AMOUNT)          AS TOTAL_REVENUE,
  SUM(QTY)                  AS TOTAL_QTY,
  COUNT(DISTINCT ORD_ID)    AS ORDER_COUNT,
  ROUND(AVG(RATING), 2)     AS AVG_RATING
 
FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME,
  PRD_ID, PRD_KEY, PRD_NAME, PRD_TYPE_NAME, CAT_ID, CAT_NAME,
  SHOP_KEY, SHOP_ID, SHOP_NAME;
/
 
 
/* ==========================================================
   REPORT 2: VW_RPT_CUSTOMER_BEHAVIOR
   พฤติกรรมการซื้อของลูกค้า
   
   Dims: DATE, USR_ID, SHOP, CATEGORY, PAY_STAT
   Measures: COUNT(ORD_ID), SUM(LINE_AMOUNT), AVG(TOTAL_AMOUNT), COUNT(DISTINCT USR_ID)
   
   Dashboard: Treemap Category by User, Scatter Freq vs AOV, Donut Payment Status
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_CUSTOMER_BEHAVIOR AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
 
  USR_ID,
 
  CAT_ID,
  CAT_NAME,
 
  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
 
  PAY_STAT_ID,
  PAY_STAT_NAME,
 
  COUNT(DISTINCT ORD_ID)    AS ORDER_COUNT,
  SUM(LINE_AMOUNT)          AS TOTAL_REVENUE,
  SUM(QTY)                  AS TOTAL_QTY,
  ROUND(AVG(TOTAL_AMOUNT), 2) AS AVG_ORDER_VALUE
 
FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME,
  USR_ID,
  CAT_ID, CAT_NAME,
  SHOP_KEY, SHOP_ID, SHOP_NAME,
  PAY_STAT_ID, PAY_STAT_NAME;
/
 
 
/* ==========================================================
   REPORT 3: VW_RPT_CAMPAIGN_EFFECTIVENESS
   ประสิทธิภาพแคมเปญ — เทียบยอดขายสินค้าใน/นอกแคมเปญ
   
   Dims: DATE, PRODUCT, CATEGORY, SHOP, CAMPAIGN
   Measures: SUM(LINE_AMOUNT), SUM(QTY), COUNT(ORD_ID), AVG(CMP_DISCOUNT)
   
   Dashboard: Grouped Bar (in vs out campaign), Gantt Timeline,
              Matrix Campaign x Product, Bar Avg Discount
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_CAMPAIGN_EFFECTIVENESS AS
SELECT
  olf.YEAR_NUM,
  olf.QUARTER_NUM,
  olf.MONTH_NUM,
  olf.MONTH_NAME,
  olf.ORDER_DATE,
 
  olf.PRD_ID,
  olf.PRD_KEY,
  olf.PRD_NAME,
  olf.CAT_ID,
  olf.CAT_NAME,
 
  olf.SHOP_KEY,
  olf.SHOP_ID,
  olf.SHOP_NAME,
 
  cp.CMP_ID,
  cp.CMP_NAME,
  cp.CMP_DISCOUNT,
  cp.CMP_START_DATE,
  cp.CMP_END_DATE,
 
  CASE
    WHEN cp.CMP_ID IS NOT NULL
         AND olf.ORDER_DATE BETWEEN cp.CMP_START_DATE AND cp.CMP_END_DATE
    THEN 'In Campaign'
    ELSE 'Not in Campaign'
  END AS CAMPAIGN_FLAG,
 
  olf.ORD_ID,
  olf.SEQ,
  olf.QTY,
  olf.UNIT_PRICE,
  olf.LINE_DISCOUNT,
  olf.LINE_AMOUNT,
  olf.RATING
 
FROM ORDER_LINE_FLAT olf
LEFT JOIN VW_DASH_CAMPAIGN_PRODUCT cp
  ON cp.PRD_ID = olf.PRD_ID;
/
 
 
/* ==========================================================
   REPORT 4: VW_RPT_SHOP_PERFORMANCE
   ประสิทธิภาพร้านค้า — ออร์เดอร์ แยกตามร้าน สถานะจัดส่ง
   
   Dims: DATE, SHOP, SHP_STAT
   Measures: COUNT(ORD_ID), SUM(QTY), SUM(LINE_AMOUNT), AVG(RATING_AVG)
   
   Dashboard: Stacked Bar Orders/Shop/Month, Shop Rating Bar,
              Heatmap Pending Orders (Shop x Month)
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SHOP_PERFORMANCE AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
  WEEK_OF_YEAR,
 
  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
  RATING_AVG                AS SHOP_RATING_AVG,
 
  SHP_STAT_ID,
  SHP_STAT_NAME,
 
  COUNT(DISTINCT ORD_ID)    AS ORDER_COUNT,
  SUM(QTY)                  AS TOTAL_QTY,
  SUM(LINE_AMOUNT)          AS TOTAL_REVENUE,
  ROUND(AVG(RATING), 2)     AS AVG_LINE_RATING
 
FROM ORDER_LINE_FLAT
GROUP BY
  YEAR_NUM, QUARTER_NUM, MONTH_NUM, MONTH_NAME, WEEK_OF_YEAR,
  SHOP_KEY, SHOP_ID, SHOP_NAME, RATING_AVG,
  SHP_STAT_ID, SHP_STAT_NAME;
/
 
 
/* ==========================================================
   REPORT 5: VW_RPT_SATISFACTION
   ความพึงพอใจ — RATING + COMMENT แยกตามร้าน สินค้า สถานะ
   
   Dims: DATE, PRODUCT, SHOP, PAY_STAT, SHP_STAT
   Measures: AVG(RATING), COUNT(RATING), COUNT(COMMENT), Distribution 1-5
   
   Dashboard: Bar Avg Rating by Shop, Histogram 1-5,
              Rating by Shipping Status, Comment Table
   ========================================================== */
CREATE OR REPLACE VIEW VW_RPT_SATISFACTION AS
SELECT
  YEAR_NUM,
  QUARTER_NUM,
  MONTH_NUM,
  MONTH_NAME,
 
  PRD_ID,
  PRD_KEY,
  PRD_NAME,
  CAT_NAME,
 
  SHOP_KEY,
  SHOP_ID,
  SHOP_NAME,
  RATING_AVG                AS SHOP_RATING_AVG,
 
  PAY_STAT_ID,
  PAY_STAT_NAME,
  SHP_STAT_ID,
  SHP_STAT_NAME,
 
  ORD_ID,
  SEQ,
  RATING,
  COMMENT_TEXT,
 
  CASE WHEN RATING IS NOT NULL THEN 1 ELSE 0 END AS HAS_RATING,
  CASE WHEN COMMENT_TEXT IS NOT NULL THEN 1 ELSE 0 END AS HAS_COMMENT,
 
  CASE
    WHEN RATING >= 4 THEN 'Good'
    WHEN RATING = 3  THEN 'Mid'
    WHEN RATING <= 2 THEN 'Low'
    ELSE 'No Rating'
  END AS RATING_GROUP
 
FROM ORDER_LINE_FLAT;
/
