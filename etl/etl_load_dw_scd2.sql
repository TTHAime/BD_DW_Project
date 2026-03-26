/* ======================================================================
   FULL SCRIPT — SCD TYPE 2 (FINAL)
   Run in DBeaver

   Architecture:
   - Staging        : TRUNCATE + full reload ทุกครั้ง
   - Lookup dims    : TRUNCATE + full reload (CATEGORY, PRD_TYPE, PAY_STAT, SHP_STAT)
   - Core dims      : INCREMENTAL (ไม่ TRUNCATE) — รักษา SCD2 history
   - Bridge         : TRUNCATE + reload ด้วย surrogate key ปัจจุบัน
   - Fact           : TRUNCATE + full reload + SCD2 join ย้อนหลัง
   ====================================================================== */

/* =========================
   0) DROP OLD OBJECTS
   ========================= */
BEGIN
  FOR x IN (
    SELECT object_name, object_type
    FROM user_objects
    WHERE object_name IN (
      'DW_FACT_ORDER_LINE','DW_FACT_ORDER',
      'DW_BRIDGE_CAMPAIGN_PRODUCT',
      'DW_DIM_DATE',
      'DW_DIM_CAMPAIGN','DW_DIM_PRODUCT','DW_DIM_SHOP',
      'DW_DIM_SHP_STAT','DW_DIM_PAY_STAT','DW_DIM_PRD_TYPE','DW_DIM_CATEGORY',
      'STG_CMP_PRD','STG_CAMPAIGN','STG_ORD_DTL','STG_ORDERS','STG_PRODUCT','STG_SHOP',
      'SEQ_DW_DIM_SHOP','SEQ_DW_DIM_PRODUCT','SEQ_DW_DIM_CAMPAIGN',
      'PR_ETL_FULL_REFRESH'
    )
  ) LOOP
    BEGIN
      IF x.object_type = 'TABLE' THEN
        EXECUTE IMMEDIATE 'DROP TABLE '||x.object_name||' CASCADE CONSTRAINTS PURGE';
      ELSIF x.object_type = 'SEQUENCE' THEN
        EXECUTE IMMEDIATE 'DROP SEQUENCE '||x.object_name;
      ELSIF x.object_type = 'PROCEDURE' THEN
        EXECUTE IMMEDIATE 'DROP PROCEDURE '||x.object_name;
      END IF;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END LOOP;
END;
/

/* =========================
   1) STAGING TABLES
   ========================= */
CREATE TABLE STG_SHOP     AS SELECT * FROM SHOP     WHERE 1=0;
CREATE TABLE STG_PRODUCT  AS SELECT * FROM PRODUCT  WHERE 1=0;
CREATE TABLE STG_ORDERS   AS SELECT * FROM ORDERS   WHERE 1=0;
CREATE TABLE STG_ORD_DTL  AS SELECT * FROM ORD_DTL  WHERE 1=0;
CREATE TABLE STG_CAMPAIGN AS SELECT * FROM CAMPAIGN  WHERE 1=0;
CREATE TABLE STG_CMP_PRD  AS SELECT * FROM CMP_PRD   WHERE 1=0;

/* =========================
   2) LOOKUP DIMS
   ========================= */
CREATE TABLE DW_DIM_CATEGORY AS SELECT * FROM CATEGORY WHERE 1=0;
CREATE TABLE DW_DIM_PRD_TYPE AS SELECT * FROM PRD_TYPE  WHERE 1=0;
CREATE TABLE DW_DIM_PAY_STAT AS SELECT * FROM PAY_STAT  WHERE 1=0;
CREATE TABLE DW_DIM_SHP_STAT AS SELECT * FROM SHP_STAT  WHERE 1=0;

/* =========================
   3) DATE DIM
   ========================= */
CREATE TABLE DW_DIM_DATE (
  DATE_KEY        NUMBER(8)    PRIMARY KEY,
  DATE_VALUE      DATE         NOT NULL,
  DAY_OF_MONTH    NUMBER(2),
  DAY_OF_WEEK_NUM NUMBER(1),
  DAY_NAME        VARCHAR2(20),
  WEEK_OF_YEAR    NUMBER(2),
  MONTH_NUM       NUMBER(2),
  MONTH_NAME      VARCHAR2(20),
  QUARTER_NUM     NUMBER(1),
  YEAR_NUM        NUMBER(4),
  IS_WEEKEND      CHAR(1) CHECK (IS_WEEKEND IN ('Y','N'))
);
CREATE INDEX IX_DW_DIM_DATE_YM ON DW_DIM_DATE (YEAR_NUM, MONTH_NUM);

/* =========================
   4) CORE DIMS — SCD TYPE 2

   - natural key (SHOP_ID / PRD_ID / CMP_ID) ซ้ำได้ = แต่ละ version
   - surrogate key (SHOP_KEY / PRD_KEY / CMP_KEY) unique ต่อ version
   - EFFECTIVE_DATE..SCD_END_DATE = ช่วงที่ version นั้น active
   - IS_CURRENT = 'Y' = version ล่าสุด
   ========================= */

-- ==================== DW_DIM_SHOP ====================
CREATE TABLE DW_DIM_SHOP (
  SHOP_KEY       NUMBER        PRIMARY KEY,
  SHOP_ID        NUMBER        NOT NULL,
  SHOP_NAME      VARCHAR2(200),
  RATING_AVG     NUMBER(5,2),
  USR_ID         NUMBER,
  CREATED_AT     DATE,
  UPDATED_AT     DATE,
  EFFECTIVE_DATE DATE          DEFAULT DATE '2000-01-01' NOT NULL,
  SCD_END_DATE   DATE          DEFAULT DATE '9999-12-31' NOT NULL,
  IS_CURRENT     CHAR(1)       DEFAULT 'Y' CHECK (IS_CURRENT IN ('Y','N'))
);
CREATE INDEX IX_DW_DIM_SHOP_CURRENT   ON DW_DIM_SHOP (SHOP_ID, IS_CURRENT);
CREATE INDEX IX_DW_DIM_SHOP_DATERANGE ON DW_DIM_SHOP (SHOP_ID, EFFECTIVE_DATE, SCD_END_DATE);

-- ==================== DW_DIM_PRODUCT ====================
CREATE TABLE DW_DIM_PRODUCT (
  PRD_KEY        NUMBER        PRIMARY KEY,
  PRD_ID         NUMBER        NOT NULL,
  PRD_NAME       VARCHAR2(200),
  DESCRIPTION    VARCHAR2(1000),
  PRICE          NUMBER(12,2),
  STOCK          NUMBER,
  DISCOUNT       NUMBER(12,2),
  CAT_ID         NUMBER,
  PRD_TYPE_ID    NUMBER,
  SHOP_ID        NUMBER,
  CREATED_AT     DATE,
  UPDATED_AT     DATE,
  EFFECTIVE_DATE DATE          DEFAULT DATE '2000-01-01' NOT NULL,
  SCD_END_DATE   DATE          DEFAULT DATE '9999-12-31' NOT NULL,
  IS_CURRENT     CHAR(1)       DEFAULT 'Y' CHECK (IS_CURRENT IN ('Y','N'))
);
CREATE INDEX IX_DW_DIM_PRODUCT_CURRENT   ON DW_DIM_PRODUCT (PRD_ID, IS_CURRENT);
CREATE INDEX IX_DW_DIM_PRODUCT_DATERANGE ON DW_DIM_PRODUCT (PRD_ID, EFFECTIVE_DATE, SCD_END_DATE);

-- ==================== DW_DIM_CAMPAIGN ====================
-- CMP_START_DATE / CMP_END_DATE = business dates (วันเริ่ม-สิ้นสุดแคมเปญ)
-- EFFECTIVE_DATE / SCD_END_DATE = SCD2 metadata (ช่วงที่ version นี้ active)
CREATE TABLE DW_DIM_CAMPAIGN (
  CMP_KEY        NUMBER        PRIMARY KEY,
  CMP_ID         NUMBER        NOT NULL,
  CMP_NAME       VARCHAR2(200),
  DISCOUNT       NUMBER(12,2),
  CMP_START_DATE DATE,
  CMP_END_DATE   DATE,
  PERIOD_ID      NUMBER,
  CREATED_AT     DATE,
  UPDATED_AT     DATE,
  EFFECTIVE_DATE DATE          DEFAULT DATE '2000-01-01' NOT NULL,
  SCD_END_DATE   DATE          DEFAULT DATE '9999-12-31' NOT NULL,
  IS_CURRENT     CHAR(1)       DEFAULT 'Y' CHECK (IS_CURRENT IN ('Y','N'))
);
CREATE INDEX IX_DW_DIM_CAMPAIGN_CURRENT   ON DW_DIM_CAMPAIGN (CMP_ID, IS_CURRENT);
CREATE INDEX IX_DW_DIM_CAMPAIGN_DATERANGE ON DW_DIM_CAMPAIGN (CMP_ID, EFFECTIVE_DATE, SCD_END_DATE);

CREATE SEQUENCE SEQ_DW_DIM_SHOP     START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DW_DIM_PRODUCT  START WITH 1 INCREMENT BY 1 NOCACHE;
CREATE SEQUENCE SEQ_DW_DIM_CAMPAIGN START WITH 1 INCREMENT BY 1 NOCACHE;

/* =========================
   5) BRIDGE
   ใช้ surrogate key เพื่อ pin campaign-product mapping
   กับ SCD version ที่ถูกต้องตอน ETL วิ่ง
   ========================= */
CREATE TABLE DW_BRIDGE_CAMPAIGN_PRODUCT (
  CMP_KEY NUMBER NOT NULL,
  PRD_KEY NUMBER NOT NULL,
  CMP_ID  NUMBER NOT NULL,
  PRD_ID  NUMBER NOT NULL,
  CONSTRAINT PK_DW_BRIDGE_CMP_PRD PRIMARY KEY (CMP_KEY, PRD_KEY)
);
CREATE INDEX IX_DW_BRIDGE_BY_PRDKEY ON DW_BRIDGE_CAMPAIGN_PRODUCT (PRD_KEY);
CREATE INDEX IX_DW_BRIDGE_BY_CMPID  ON DW_BRIDGE_CAMPAIGN_PRODUCT (CMP_ID);
CREATE INDEX IX_DW_BRIDGE_BY_PRDID  ON DW_BRIDGE_CAMPAIGN_PRODUCT (PRD_ID);

/* =========================
   6) FACT TABLES
   ========================= */
CREATE TABLE DW_FACT_ORDER (
  ORD_ID         NUMBER PRIMARY KEY,
  ORDER_DATE     DATE,
  ORDER_DATE_KEY NUMBER(8),
  USR_ID         NUMBER,
  PAY_STAT_ID    NUMBER,
  TOTAL_AMOUNT   NUMBER(12,2),
  TOTAL_DISCOUNT NUMBER(12,2)
);

CREATE TABLE DW_FACT_ORDER_LINE (
  ORD_ID         NUMBER       NOT NULL,
  SEQ            NUMBER       NOT NULL,
  ORDER_DATE     DATE,
  ORDER_DATE_KEY NUMBER(8),
  USR_ID         NUMBER,
  SHOP_KEY       NUMBER,
  PRD_KEY        NUMBER,
  SHP_STAT_ID    NUMBER,
  QTY            NUMBER,
  UNIT_PRICE     NUMBER(12,2),
  DISCOUNT       NUMBER(12,2),
  LINE_AMOUNT    NUMBER(12,2),
  RATING         NUMBER,
  COMMENT_TEXT   VARCHAR2(1000),
  CONSTRAINT PK_DW_FACT_ORDER_LINE PRIMARY KEY (ORD_ID, SEQ)
);

/* =========================
   7) INDEXES — FACT
   ========================= */
CREATE INDEX IX_DW_FACT_ORDER_DATEKEY ON DW_FACT_ORDER (ORDER_DATE_KEY);
CREATE INDEX IX_DW_FACT_ORDER_PAYSTAT ON DW_FACT_ORDER (PAY_STAT_ID);
CREATE INDEX IX_DW_FACT_ORDER_USR     ON DW_FACT_ORDER (USR_ID);

CREATE INDEX IX_DW_FOL_DATEKEY ON DW_FACT_ORDER_LINE (ORDER_DATE_KEY);
CREATE INDEX IX_DW_FOL_PRDKEY  ON DW_FACT_ORDER_LINE (PRD_KEY);
CREATE INDEX IX_DW_FOL_SHOPKEY ON DW_FACT_ORDER_LINE (SHOP_KEY);
CREATE INDEX IX_DW_FOL_SHPSTAT ON DW_FACT_ORDER_LINE (SHP_STAT_ID);
CREATE INDEX IX_DW_FOL_USR     ON DW_FACT_ORDER_LINE (USR_ID);

/* =========================
   8) ETL PROCEDURE
   ========================= */
CREATE OR REPLACE PROCEDURE PR_ETL_FULL_REFRESH AS
  v_min_date     DATE;
  v_max_date     DATE;
  v_d            DATE;
  v_today        DATE := TRUNC(SYSDATE);
  v_initial_date DATE := DATE '2000-01-01';
BEGIN

  /* ---------- STEP 1: STAGING ---------- */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_SHOP';
  INSERT INTO STG_SHOP     SELECT * FROM SHOP;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_PRODUCT';
  INSERT INTO STG_PRODUCT  SELECT * FROM PRODUCT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_ORDERS';
  INSERT INTO STG_ORDERS   SELECT * FROM ORDERS;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_ORD_DTL';
  INSERT INTO STG_ORD_DTL  SELECT * FROM ORD_DTL;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_CAMPAIGN';
  INSERT INTO STG_CAMPAIGN SELECT * FROM CAMPAIGN;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE STG_CMP_PRD';
  INSERT INTO STG_CMP_PRD  SELECT * FROM CMP_PRD;

  /* ---------- STEP 2: LOOKUP DIMS (full refresh) ---------- */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DIM_CATEGORY';
  INSERT INTO DW_DIM_CATEGORY SELECT * FROM CATEGORY;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DIM_PRD_TYPE';
  INSERT INTO DW_DIM_PRD_TYPE SELECT * FROM PRD_TYPE;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DIM_PAY_STAT';
  INSERT INTO DW_DIM_PAY_STAT SELECT * FROM PAY_STAT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DIM_SHP_STAT';
  INSERT INTO DW_DIM_SHP_STAT SELECT * FROM SHP_STAT;

  /* ---------- STEP 3: DATE DIM ----------
     populate ตั้งแต่วัน order แรก ถึงวัน campaign สุดท้าย
     ---------------------------------------- */
  SELECT
    LEAST(
      NVL(TRUNC(MIN(o.ORDER_DATE)), DATE '2999-12-31'),
      NVL(TRUNC(MIN(c.START_DATE)), DATE '2999-12-31')
    ),
    GREATEST(
      NVL(TRUNC(MAX(o.ORDER_DATE)), DATE '1900-01-01'),
      NVL(TRUNC(MAX(c.END_DATE)),   DATE '1900-01-01')
    )
  INTO v_min_date, v_max_date
  FROM STG_ORDERS o
  CROSS JOIN (
    SELECT MIN(START_DATE) AS START_DATE, MAX(END_DATE) AS END_DATE
    FROM STG_CAMPAIGN
  ) c;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_DIM_DATE';

  IF v_min_date <= v_max_date THEN
    v_d := v_min_date;
    WHILE v_d <= v_max_date LOOP
      INSERT INTO DW_DIM_DATE (
        DATE_KEY, DATE_VALUE,
        DAY_OF_MONTH, DAY_OF_WEEK_NUM, DAY_NAME,
        WEEK_OF_YEAR, MONTH_NUM, MONTH_NAME,
        QUARTER_NUM, YEAR_NUM, IS_WEEKEND
      ) VALUES (
        TO_NUMBER(TO_CHAR(v_d, 'YYYYMMDD')),
        v_d,
        TO_NUMBER(TO_CHAR(v_d, 'DD')),
        TO_NUMBER(TO_CHAR(v_d, 'D')),
        TRIM(TO_CHAR(v_d, 'DAY')),
        TO_NUMBER(TO_CHAR(v_d, 'IW')),
        TO_NUMBER(TO_CHAR(v_d, 'MM')),
        TRIM(TO_CHAR(v_d, 'MONTH')),
        TO_NUMBER(TO_CHAR(v_d, 'Q')),
        TO_NUMBER(TO_CHAR(v_d, 'YYYY')),
        CASE
          WHEN TO_CHAR(v_d, 'DY', 'NLS_DATE_LANGUAGE=ENGLISH') IN ('SAT','SUN')
          THEN 'Y' ELSE 'N'
        END
      );
      v_d := v_d + 1;
    END LOOP;
  END IF;

  /* ---------- STEP 4: SCD2 — DW_DIM_SHOP ----------
     Step A: ปิด version ที่ข้อมูลเปลี่ยน
     Step B: เพิ่ม version ใหม่ (changed + brand new)
     -------------------------------------------------- */

  -- A: Close changed rows
  UPDATE DW_DIM_SHOP d
  SET    d.SCD_END_DATE = v_today - 1,
         d.IS_CURRENT   = 'N'
  WHERE  d.IS_CURRENT = 'Y'
    AND  EXISTS (
           SELECT 1 FROM STG_SHOP s
           WHERE  s.SHOP_ID = d.SHOP_ID
             AND  (   UPPER(TRIM(s.SHOP_NAME))     != NVL(d.SHOP_NAME, '~')
                   OR ROUND(NVL(s.RATING_AVG,0),2) != NVL(d.RATING_AVG, 0)
                   OR NVL(s.USR_ID, 0)             != NVL(d.USR_ID, 0)
                  )
         );

  -- B: Insert new version
  INSERT INTO DW_DIM_SHOP (
    SHOP_KEY, SHOP_ID, SHOP_NAME, RATING_AVG, USR_ID,
    CREATED_AT, UPDATED_AT, EFFECTIVE_DATE, SCD_END_DATE, IS_CURRENT
  )
  SELECT
    SEQ_DW_DIM_SHOP.NEXTVAL,
    s.SHOP_ID,
    UPPER(TRIM(s.SHOP_NAME)),
    ROUND(NVL(s.RATING_AVG, 0), 2),
    s.USR_ID,
    s.CREATED_AT,
    s.UPDATED_AT,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM DW_DIM_SHOP x WHERE x.SHOP_ID = s.SHOP_ID)
      THEN v_initial_date   -- brand new: ครอบ order ทุกวันในอดีต
      ELSE v_today          -- changed: เริ่มนับจากวันนี้
    END,
    DATE '9999-12-31',
    'Y'
  FROM STG_SHOP s
  WHERE NOT EXISTS (
    SELECT 1 FROM DW_DIM_SHOP d
    WHERE  d.SHOP_ID = s.SHOP_ID AND d.IS_CURRENT = 'Y'
  );

  /* ---------- STEP 5: SCD2 — DW_DIM_PRODUCT ---------- */

  -- A: Close changed rows
  UPDATE DW_DIM_PRODUCT d
  SET    d.SCD_END_DATE = v_today - 1,
         d.IS_CURRENT   = 'N'
  WHERE  d.IS_CURRENT = 'Y'
    AND  EXISTS (
           SELECT 1 FROM STG_PRODUCT s
           WHERE  s.PRD_ID = d.PRD_ID
             AND  (   UPPER(TRIM(s.NAME))                        != NVL(d.PRD_NAME, '~')
                   OR ROUND(GREATEST(NVL(s.PRICE,0),0),2)        != NVL(d.PRICE, 0)
                   OR GREATEST(NVL(s.STOCK,0),0)                 != NVL(d.STOCK, 0)
                   OR ROUND(GREATEST(NVL(s.DISCOUNT,0),0),2)     != NVL(d.DISCOUNT, 0)
                   OR NVL(s.CAT_ID, 0)                           != NVL(d.CAT_ID, 0)
                   OR NVL(s.PRD_TYPE_ID, 0)                      != NVL(d.PRD_TYPE_ID, 0)
                   OR NVL(s.SHOP_ID, 0)                          != NVL(d.SHOP_ID, 0)
                  )
         );

  -- B: Insert new version
  INSERT INTO DW_DIM_PRODUCT (
    PRD_KEY, PRD_ID, PRD_NAME, DESCRIPTION,
    PRICE, STOCK, DISCOUNT,
    CAT_ID, PRD_TYPE_ID, SHOP_ID,
    CREATED_AT, UPDATED_AT, EFFECTIVE_DATE, SCD_END_DATE, IS_CURRENT
  )
  SELECT
    SEQ_DW_DIM_PRODUCT.NEXTVAL,
    s.PRD_ID,
    UPPER(TRIM(s.NAME)),
    NULLIF(TRIM(s.DESCRIPTION), ''),
    ROUND(GREATEST(NVL(s.PRICE,0),0), 2),
    GREATEST(NVL(s.STOCK,0),0),
    ROUND(GREATEST(NVL(s.DISCOUNT,0),0), 2),
    s.CAT_ID,
    s.PRD_TYPE_ID,
    s.SHOP_ID,
    s.CREATED_AT,
    s.UPDATED_AT,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM DW_DIM_PRODUCT x WHERE x.PRD_ID = s.PRD_ID)
      THEN v_initial_date
      ELSE v_today
    END,
    DATE '9999-12-31',
    'Y'
  FROM STG_PRODUCT s
  WHERE NOT EXISTS (
    SELECT 1 FROM DW_DIM_PRODUCT d
    WHERE  d.PRD_ID = s.PRD_ID AND d.IS_CURRENT = 'Y'
  );

  /* ---------- STEP 6: SCD2 — DW_DIM_CAMPAIGN ----------
     CMP_START_DATE/CMP_END_DATE = business dates
     EFFECTIVE_DATE/SCD_END_DATE = SCD2 version metadata
     ------------------------------------------------------ */

  -- A: Close changed rows
  UPDATE DW_DIM_CAMPAIGN d
  SET    d.SCD_END_DATE = v_today - 1,
         d.IS_CURRENT   = 'N'
  WHERE  d.IS_CURRENT = 'Y'
    AND  EXISTS (
           SELECT 1 FROM STG_CAMPAIGN s
           WHERE  s.CMP_ID = d.CMP_ID
             AND  (   UPPER(TRIM(s.NAME))                        != NVL(d.CMP_NAME, '~')
                   OR ROUND(GREATEST(NVL(s.DISCOUNT,0),0),2)     != NVL(d.DISCOUNT, 0)
                   OR NVL(s.START_DATE, DATE '1900-01-01')        != NVL(d.CMP_START_DATE, DATE '1900-01-01')
                   OR NVL(s.END_DATE,   DATE '1900-01-01')        != NVL(d.CMP_END_DATE,   DATE '1900-01-01')
                   OR NVL(s.PERIOD_ID, 0)                        != NVL(d.PERIOD_ID, 0)
                  )
         );

  -- B: Insert new version
  INSERT INTO DW_DIM_CAMPAIGN (
    CMP_KEY, CMP_ID, CMP_NAME, DISCOUNT,
    CMP_START_DATE, CMP_END_DATE, PERIOD_ID,
    CREATED_AT, UPDATED_AT, EFFECTIVE_DATE, SCD_END_DATE, IS_CURRENT
  )
  SELECT
    SEQ_DW_DIM_CAMPAIGN.NEXTVAL,
    s.CMP_ID,
    UPPER(TRIM(s.NAME)),
    ROUND(GREATEST(NVL(s.DISCOUNT,0),0), 2),
    s.START_DATE,
    s.END_DATE,
    s.PERIOD_ID,
    s.CREATED_AT,
    s.UPDATED_AT,
    CASE
      WHEN NOT EXISTS (SELECT 1 FROM DW_DIM_CAMPAIGN x WHERE x.CMP_ID = s.CMP_ID)
      THEN v_initial_date
      ELSE v_today
    END,
    DATE '9999-12-31',
    'Y'
  FROM STG_CAMPAIGN s
  WHERE NOT EXISTS (
    SELECT 1 FROM DW_DIM_CAMPAIGN d
    WHERE  d.CMP_ID = s.CMP_ID AND d.IS_CURRENT = 'Y'
  );

  /* ---------- STEP 7: BRIDGE (full refresh ด้วย surrogate key) ----------
     join กับ current version → ได้ CMP_KEY/PRD_KEY ที่ถูกต้อง
     --------------------------------------------------------------------- */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_BRIDGE_CAMPAIGN_PRODUCT';
  INSERT INTO DW_BRIDGE_CAMPAIGN_PRODUCT (CMP_KEY, PRD_KEY, CMP_ID, PRD_ID)
  SELECT c.CMP_KEY, p.PRD_KEY, cp.CMP_ID, cp.PRD_ID
  FROM   STG_CMP_PRD cp
  JOIN   DW_DIM_CAMPAIGN c ON c.CMP_ID = cp.CMP_ID AND c.IS_CURRENT = 'Y'
  JOIN   DW_DIM_PRODUCT  p ON p.PRD_ID = cp.PRD_ID AND p.IS_CURRENT = 'Y';

  /* ---------- STEP 8: FACT ORDER HEADER (full refresh) ---------- */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_FACT_ORDER';
  INSERT INTO DW_FACT_ORDER (
    ORD_ID, ORDER_DATE, ORDER_DATE_KEY,
    USR_ID, PAY_STAT_ID, TOTAL_AMOUNT, TOTAL_DISCOUNT
  )
  SELECT
    ORD_ID,
    ORDER_DATE,
    CASE WHEN ORDER_DATE IS NULL THEN NULL
         ELSE TO_NUMBER(TO_CHAR(TRUNC(ORDER_DATE), 'YYYYMMDD'))
    END,
    USR_ID,
    PAY_STAT_ID,
    ROUND(GREATEST(NVL(TOTAL_AMOUNT,0),0), 2),
    ROUND(GREATEST(NVL(TOTAL_DISCOUNT,0),0), 2)
  FROM STG_ORDERS;

  /* ---------- STEP 9: FACT ORDER LINE (full refresh + SCD2 join) ----------

     SCD2 join: หา surrogate key ของ dim version ที่ active ณ ORDER_DATE
       TRUNC(ORDER_DATE) BETWEEN EFFECTIVE_DATE AND SCD_END_DATE

     Fallback: ถ้าหา version ไม่ได้ (gap ในข้อมูล) → ใช้ current version
       เพื่อไม่ให้ fact row หายออกจากระบบ

     LINE_AMOUNT = QTY * UNIT_PRICE - DISCOUNT (ระดับ line)
     ----------------------------------------------------------------------- */
  EXECUTE IMMEDIATE 'TRUNCATE TABLE DW_FACT_ORDER_LINE';
  INSERT INTO DW_FACT_ORDER_LINE (
    ORD_ID, SEQ, ORDER_DATE, ORDER_DATE_KEY, USR_ID,
    SHOP_KEY, PRD_KEY,
    SHP_STAT_ID, QTY, UNIT_PRICE, DISCOUNT,
    LINE_AMOUNT, RATING, COMMENT_TEXT
  )
  SELECT
    d.ORD_ID,
    d.SEQ,
    o.ORDER_DATE,
    CASE WHEN o.ORDER_DATE IS NULL THEN NULL
         ELSE TO_NUMBER(TO_CHAR(TRUNC(o.ORDER_DATE), 'YYYYMMDD'))
    END                                                              AS ORDER_DATE_KEY,
    o.USR_ID,
    -- SHOP_KEY: version active ณ order date; fallback → current
    COALESCE(
      (SELECT sdim.SHOP_KEY FROM DW_DIM_SHOP sdim
       WHERE  sdim.SHOP_ID = p.SHOP_ID
         AND  TRUNC(o.ORDER_DATE) BETWEEN sdim.EFFECTIVE_DATE AND sdim.SCD_END_DATE
         AND  ROWNUM = 1),
      (SELECT sdim.SHOP_KEY FROM DW_DIM_SHOP sdim
       WHERE  sdim.SHOP_ID = p.SHOP_ID AND sdim.IS_CURRENT = 'Y' AND ROWNUM = 1)
    )                                                                AS SHOP_KEY,
    -- PRD_KEY: version active ณ order date; fallback → current
    COALESCE(
      (SELECT pdim.PRD_KEY FROM DW_DIM_PRODUCT pdim
       WHERE  pdim.PRD_ID = d.PRD_ID
         AND  TRUNC(o.ORDER_DATE) BETWEEN pdim.EFFECTIVE_DATE AND pdim.SCD_END_DATE
         AND  ROWNUM = 1),
      (SELECT pdim.PRD_KEY FROM DW_DIM_PRODUCT pdim
       WHERE  pdim.PRD_ID = d.PRD_ID AND pdim.IS_CURRENT = 'Y' AND ROWNUM = 1)
    )                                                                AS PRD_KEY,
    d.SHP_STAT_ID,
    GREATEST(NVL(d.QTY,0),0)                                        AS QTY,
    ROUND(GREATEST(NVL(d.UNIT_PRICE,0),0), 2)                       AS UNIT_PRICE,
    ROUND(GREATEST(NVL(d.DISCOUNT,0),0), 2)                         AS DISCOUNT,
    ROUND(
      GREATEST(NVL(d.QTY,0),0) * GREATEST(NVL(d.UNIT_PRICE,0),0)
      - GREATEST(NVL(d.DISCOUNT,0),0)
    , 2)                                                             AS LINE_AMOUNT,
    d.RATING,
    NULLIF(TRIM(d.COMMENT_TEXT), '')
  FROM  STG_ORD_DTL d
  JOIN  STG_ORDERS  o ON o.ORD_ID = d.ORD_ID
  JOIN  STG_PRODUCT p ON p.PRD_ID = d.PRD_ID;

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
    ROLLBACK;
    RAISE;
END;
/

/* =========================
   9) RUN ETL
   ========================= */
BEGIN
  PR_ETL_FULL_REFRESH;
END;
/
