--------------------------------------------------------------------------------
-- Order Line Flat — SCD TYPE 2 VERSION
-- Key change: JOIN dims on date range (not simple NK)
-- This gives the CORRECT dimension version for each order's date
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW ORDER_LINE_FLAT AS
SELECT
  /* Date */
  fo.ORDER_DATE_KEY,
  dd.DATE_VALUE,
  dd.MONTH_NUM,
  dd.MONTH_NAME,
  dd.QUARTER_NUM,
  dd.YEAR_NUM,
  dd.IS_WEEKEND,

  /* Category */
  cat.CAT_ID,
  cat.NAME                                    AS CAT_NAME,

  /* Line */
  fol.COMMENT_TEXT,
  pdim.DESCRIPTION,
  fol.LINE_AMOUNT,
  fol.DISCOUNT                                AS LINE_DISCOUNT,
  (NVL(fol.QTY,0) * NVL(fol.UNIT_PRICE,0))   AS LIST_PRICE,

  /* Order */
  fol.ORD_ID,
  fo.ORDER_DATE,
  fo.PAY_STAT_ID,
  pay.NAME                                    AS PAY_STAT_NAME,

  /* Product (version at order date) */
  pdim.DISCOUNT                               AS PRD_DISCOUNT,
  pdim.PRD_ID,
  fol.PRD_KEY,
  pdim.PRD_NAME,
  pdim.PRICE                                  AS PRD_PRICE,
  pdim.STOCK                                  AS PRD_STOCK,
  pdim.PRD_TYPE_ID,
  ptype.NAME                                  AS PRD_TYPE_NAME,

  /* Product SCD2 metadata */
  pdim.EFFECTIVE_DATE                         AS PRD_EFFECTIVE_DATE,
  pdim.END_DATE                               AS PRD_END_DATE,
  pdim.IS_CURRENT                             AS PRD_IS_CURRENT,

  /* More line fields */
  fol.QTY,
  fol.RATING,

  /* Shop (version at order date) */
  sdim.RATING_AVG,
  fol.SEQ,
  sdim.SHOP_ID,
  fol.SHOP_KEY,
  sdim.SHOP_NAME,

  /* Shop SCD2 metadata */
  sdim.EFFECTIVE_DATE                         AS SHOP_EFFECTIVE_DATE,
  sdim.END_DATE                               AS SHOP_END_DATE,
  sdim.IS_CURRENT                             AS SHOP_IS_CURRENT,

  /* Shipping status */
  fol.SHP_STAT_ID,
  shp.NAME                                    AS SHP_STAT_NAME,

  /* Order totals */
  fo.TOTAL_AMOUNT,
  fo.TOTAL_DISCOUNT,

  /* Unit price + user */
  fol.UNIT_PRICE,
  fo.USR_ID

FROM DW_FACT_ORDER_LINE fol
JOIN DW_FACT_ORDER fo
  ON fo.ORD_ID = fol.ORD_ID
LEFT JOIN DW_DIM_DATE dd
  ON dd.DATE_KEY = fo.ORDER_DATE_KEY
/* SCD2: PRD_KEY in fact already points to the correct version */
LEFT JOIN DW_DIM_PRODUCT pdim
  ON pdim.PRD_KEY = fol.PRD_KEY
/* SCD2: SHOP_KEY in fact already points to the correct version */
LEFT JOIN DW_DIM_SHOP sdim
  ON sdim.SHOP_KEY = fol.SHOP_KEY
LEFT JOIN DW_DIM_CATEGORY cat
  ON cat.CAT_ID = pdim.CAT_ID
LEFT JOIN DW_DIM_PRD_TYPE ptype
  ON ptype.PRD_TYPE_ID = pdim.PRD_TYPE_ID
LEFT JOIN DW_DIM_PAY_STAT pay
  ON pay.PAY_STAT_ID = fo.PAY_STAT_ID
LEFT JOIN DW_DIM_SHP_STAT shp
  ON shp.SHP_STAT_ID = fol.SHP_STAT_ID
;
/

--------------------------------------------------------------------------------
-- Current Product View — convenience view for "latest" product info
-- Use this when you need current product data (e.g., dashboard filters)
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DIM_PRODUCT_CURRENT AS
SELECT *
FROM DW_DIM_PRODUCT
WHERE IS_CURRENT = 'Y';
/

--------------------------------------------------------------------------------
-- Current Shop View
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DIM_SHOP_CURRENT AS
SELECT *
FROM DW_DIM_SHOP
WHERE IS_CURRENT = 'Y';
/

--------------------------------------------------------------------------------
-- Campaign Product (unchanged — bridge uses natural keys)
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DASH_CAMPAIGN_PRODUCT AS
SELECT
  c.CMP_ID,
  c.CMP_NAME,
  c.DISCOUNT  AS CMP_DISCOUNT,
  c.START_DATE,
  c.END_DATE,
  c.PERIOD_ID,

  p.PRD_KEY,
  p.PRD_ID,
  p.PRD_NAME,
  p.CAT_ID,
  p.PRD_TYPE_ID

FROM DW_BRIDGE_CAMPAIGN_PRODUCT b
JOIN DW_DIM_CAMPAIGN c
  ON c.CMP_ID = b.CMP_ID
  AND c.IS_CURRENT = 'Y'
JOIN DW_DIM_PRODUCT p
  ON p.PRD_ID = b.PRD_ID
  AND p.IS_CURRENT = 'Y'
;
/
