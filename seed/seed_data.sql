-- =========================================================
-- FULL SEED SCRIPT (Start IDs from 1) - Oracle / DBeaver
-- Tables:
--   USR_TYPE, APP_USER, CATEGORY, PRD_TYPE, PAY_STAT, SHP_STAT, PERIOD,
--   SHOP, PRODUCT, ORDERS, ORD_DTL, CAMPAIGN, CMP_PRD
-- Triggers assumed:
--   TRG_ORD_DTL_SEQ, TRG_CMP_PRD_SEQ
-- =========================================================

SELECT * FROM PRODUCT p 
-- =========================================================
-- 0) RESET DATA (delete children first)
-- =========================================================
BEGIN
  EXECUTE IMMEDIATE 'DELETE FROM CMP_PRD';
  EXECUTE IMMEDIATE 'DELETE FROM CAMPAIGN';
  EXECUTE IMMEDIATE 'DELETE FROM ORD_DTL';
  EXECUTE IMMEDIATE 'DELETE FROM ORDERS';
  EXECUTE IMMEDIATE 'DELETE FROM PRODUCT';
  EXECUTE IMMEDIATE 'DELETE FROM SHOP';

  EXECUTE IMMEDIATE 'DELETE FROM PERIOD';
  EXECUTE IMMEDIATE 'DELETE FROM SHP_STAT';
  EXECUTE IMMEDIATE 'DELETE FROM PAY_STAT';
  EXECUTE IMMEDIATE 'DELETE FROM PRD_TYPE';
  EXECUTE IMMEDIATE 'DELETE FROM CATEGORY';
  EXECUTE IMMEDIATE 'DELETE FROM APP_USER';
  EXECUTE IMMEDIATE 'DELETE FROM USR_TYPE';

  COMMIT;
  DBMS_OUTPUT.PUT_LINE('RESET DONE');
EXCEPTION
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('RESET SKIP/ERROR: '||SQLERRM);
END;
/
COMMIT;

-- =========================================================
-- 1) MASTER SEEDS
-- =========================================================

-- 1.1 USR_TYPE
INSERT INTO USR_TYPE (USR_TYPE_ID, NAME) VALUES (1,'Admin');
INSERT INTO USR_TYPE (USR_TYPE_ID, NAME) VALUES (2,'Customer');
INSERT INTO USR_TYPE (USR_TYPE_ID, NAME) VALUES (3,'Seller');

-- 1.2 PAY_STAT: 1 Pending, 2 Paid, 3 Cancelled
INSERT INTO PAY_STAT (PAY_STAT_ID, NAME) VALUES (1,'Pending');
INSERT INTO PAY_STAT (PAY_STAT_ID, NAME) VALUES (2,'Paid');
INSERT INTO PAY_STAT (PAY_STAT_ID, NAME) VALUES (3,'Cancelled');

-- 1.3 SHP_STAT: 1 Cancelled, 2 Shipping, 3 Delivered
INSERT INTO SHP_STAT (SHP_STAT_ID, NAME) VALUES (1,'Cancelled');
INSERT INTO SHP_STAT (SHP_STAT_ID, NAME) VALUES (2,'Shipping');
INSERT INTO SHP_STAT (SHP_STAT_ID, NAME) VALUES (3,'Delivered');

-- 1.4 CATEGORY
INSERT INTO CATEGORY (CAT_ID, NAME) VALUES (1,'Fruit');
INSERT INTO CATEGORY (CAT_ID, NAME) VALUES (2,'Vegetable');
INSERT INTO CATEGORY (CAT_ID, NAME) VALUES (3,'Processed');

-- 1.5 PERIOD
INSERT INTO PERIOD (PERIOD_ID, NAME, START_DATE, END_DATE)
VALUES (1, 'Summer Harvest Season', DATE '2026-03-01', DATE '2026-05-31');

INSERT INTO PERIOD (PERIOD_ID, NAME, START_DATE, END_DATE)
VALUES (2, 'Rainy Season Promotion', DATE '2026-06-01', DATE '2026-10-31');

COMMIT;

-- =========================================================
-- 2) APP_USER (Admin + Customers + Sellers)
-- =========================================================
-- Admin (id=1)
INSERT INTO APP_USER (USR_ID, USERNAME, PASSWORD, NAME, EMAIL, ADDRESS1, USR_TYPE_ID)
VALUES (1, 'admin', 'pass123', 'Admin', 'admin@mail.com', 'Bangkok', 1);

BEGIN
  -- Customers: 2..21 (20 คน)
  FOR i IN 2..21 LOOP
    INSERT INTO APP_USER
      (USR_ID, USERNAME, PASSWORD, NAME, EMAIL, ADDRESS1, TEL1, USR_TYPE_ID)
    VALUES
      (i,
       'cust' || i,
       'pass123',
       'Customer_' || i,
       'cust' || i || '@mail.com',
       'Bangkok',
       '08' || LPAD(i,8,'0'),
       2);
  END LOOP;

  -- Sellers: 22..41 (20 คน)
  FOR i IN 22..41 LOOP
    INSERT INTO APP_USER
      (USR_ID, USERNAME, PASSWORD, NAME, EMAIL, ADDRESS1, TEL1, USR_TYPE_ID)
    VALUES
      (i,
       'seller' || i,
       'pass123',
       'Seller_' || i,
       'seller' || i || '@mail.com',
       'Bangkok',
       '09' || LPAD(i,8,'0'),
       3);
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- 3) SHOP (1..20) ผูกกับ seller 20 คนแรก
-- =========================================================
DECLARE
  v_shop_id NUMBER := 1;
  v_counter NUMBER := 0;

  TYPE shop_array IS VARRAY(20) OF VARCHAR2(100);
  v_names shop_array := shop_array(
    'Fresh Farm Market','Golden Mango Orchard','Organic Harvest Hub','Green Valley Produce',
    'Tropical Fruit Garden','Thai Agro Fresh','Sunrise Vegetable Farm','Premium Durian House',
    'Healthy Crop Market','Farm Direct Thailand','Sweet Banana Orchard','เชียงใหม่ผักสด',
    'สวนผลไม้ลุงสมชาย','ตลาดเกษตรอินทรีย์','ฟาร์มผักปลอดสาร','ราชาผลไม้ไทย',
    'สวนมะม่วงทอง','ฟาร์มผักไฮโดร','ตลาดผลไม้สดใหม่','สวนเกษตรรุ่งเรือง'
  );
BEGIN
  FOR rec IN (
    SELECT USR_ID
    FROM APP_USER
    WHERE USR_TYPE_ID = 3
    ORDER BY USR_ID
    FETCH FIRST 20 ROWS ONLY
  ) LOOP
    v_counter := v_counter + 1;

    INSERT INTO SHOP (SHOP_ID, SHOP_NAME, RATING_AVG, USR_ID)
    VALUES (
      v_shop_id,
      v_names(v_counter),
      CASE WHEN DBMS_RANDOM.VALUE(0,1) < 0.3
           THEN ROUND(DBMS_RANDOM.VALUE(3.8,4.3),2)
           ELSE ROUND(DBMS_RANDOM.VALUE(4.3,4.9),2)
      END,
      rec.USR_ID
    );

    v_shop_id := v_shop_id + 1;
  END LOOP;
END;

COMMIT;

-- =========================================================
-- 4) PRD_TYPE (เริ่มที่ 1)  *** ต้องมีคอลัมน์ CAT_ID ใน PRD_TYPE ***
-- Columns: (PRD_TYPE_ID, CAT_ID, NAME)
-- =========================================================
BEGIN
  -- Fruit (CAT_ID=1)
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (1, 1, 'Mango');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (2, 1, 'Durian');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (3, 1, 'Banana');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (4, 1, 'Papaya');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (5, 1, 'Guava');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (6, 1, 'Pineapple');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (7, 1, 'Longan');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (8, 1, 'Rambutan');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (9, 1, 'Coconut');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (10,1, 'Watermelon');

  -- Vegetable (CAT_ID=2)
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (11,2, 'Cabbage');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (12,2, 'Carrot');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (13,2, 'Broccoli');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (14,2, 'Chili');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (15,2, 'Tomato');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (16,2, 'Cucumber');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (17,2, 'Pumpkin');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (18,2, 'Morning Glory');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (19,2, 'Eggplant');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (20,2, 'Onion');

  -- Processed (CAT_ID=3)
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (21,3, 'Dried Mango');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (22,3, 'Banana Chips');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (23,3, 'Chili Paste');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (24,3, 'Pickled Cabbage');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (25,3, 'Tomato Sauce');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (26,3, 'Coconut Milk');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (27,3, 'Pumpkin Chips');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (28,3, 'Fried Shallot');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (29,3, 'Garlic Paste');
  INSERT INTO PRD_TYPE (PRD_TYPE_ID, CAT_ID, NAME) VALUES (30,3, 'Fruit Jam');
END;
/
COMMIT;

-- =========================================================
-- 5) PRODUCT (1..200) - สุ่มให้สัมพันธ์ CAT/TYPE/SHOP
-- =========================================================
DECLARE
  v_cat_id NUMBER;
  v_type_id NUMBER;
  v_type_name VARCHAR2(100);
  v_shop_id NUMBER;
BEGIN
  FOR i IN 1..200 LOOP
    v_cat_id := TRUNC(DBMS_RANDOM.VALUE(1,4));   -- 1..3
    v_shop_id := TRUNC(DBMS_RANDOM.VALUE(1,21)); -- 1..20

    SELECT PRD_TYPE_ID, NAME
      INTO v_type_id, v_type_name
      FROM (
        SELECT PRD_TYPE_ID, NAME
        FROM PRD_TYPE
        WHERE CAT_ID = v_cat_id
        ORDER BY DBMS_RANDOM.VALUE
      )
     WHERE ROWNUM = 1;

    INSERT INTO PRODUCT
  (PRD_ID, NAME, DESCRIPTION, PRICE, STOCK, DISCOUNT, CAT_ID, PRD_TYPE_ID, SHOP_ID)
VALUES
  (i,
   v_type_name,                           -- ✅ ชื่อสินค้า ไม่ต้องมี #num
   'ข้อมูลสินค้า: ' || v_type_name,       -- ✅ description เป็นข้อมูลสินค้า
   TRUNC(DBMS_RANDOM.VALUE(20,200)) + 0.99,
   TRUNC(DBMS_RANDOM.VALUE(10,500)),
   TRUNC(DBMS_RANDOM.VALUE(0,20)),
   v_cat_id,
   v_type_id,
   v_shop_id);
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- 6) ORDERS (1..200) - ผูก customer (2..21)
-- =========================================================
DECLARE
  v_total NUMBER;
  v_discount NUMBER;
  v_pay_stat NUMBER;
  v_user NUMBER;
BEGIN
  FOR i IN 1..200 LOOP
    v_user := TRUNC(DBMS_RANDOM.VALUE(2,22)); -- customers: 2..21

    v_total := TRUNC(DBMS_RANDOM.VALUE(200,5000));
    v_discount := TRUNC(v_total * DBMS_RANDOM.VALUE(0,0.2));

    IF DBMS_RANDOM.VALUE < 0.7 THEN
      v_pay_stat := 2; -- Paid
    ELSIF DBMS_RANDOM.VALUE < 0.9 THEN
      v_pay_stat := 1; -- Pending
    ELSE
      v_pay_stat := 3; -- Cancelled
    END IF;

    INSERT INTO ORDERS
      (ORD_ID, ORDER_DATE, TOTAL_AMOUNT, TOTAL_DISCOUNT, USR_ID, PAY_STAT_ID)
    VALUES
      (i,
       SYSDATE - TRUNC(DBMS_RANDOM.VALUE(0,60)),
       v_total,
       v_discount,
       v_user,
       v_pay_stat);
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- 7) ORD_DTL - ให้ trigger ใส่ SEQ เอง
--   - เลือก product 1..200
--   - สถานะส่ง: 1 Cancelled, 2 Shipping, 3 Delivered
-- =========================================================
DECLARE
  v_price PRODUCT.PRICE%TYPE;
  v_qty NUMBER;
  v_discount NUMBER;
  v_ship_status NUMBER;
  v_prd_id NUMBER;
  v_item_count NUMBER;
BEGIN
  FOR o IN (SELECT ORD_ID, PAY_STAT_ID FROM ORDERS WHERE ORD_ID BETWEEN 1 AND 200) LOOP
    v_item_count := TRUNC(DBMS_RANDOM.VALUE(1,6)); -- 1..5 lines

    FOR j IN 1..v_item_count LOOP
      v_prd_id := TRUNC(DBMS_RANDOM.VALUE(1,201)); -- 1..200

      SELECT PRICE INTO v_price
      FROM PRODUCT
      WHERE PRD_ID = v_prd_id;

      v_qty := TRUNC(DBMS_RANDOM.VALUE(1,6)); -- 1..5
      v_discount := TRUNC(v_price * DBMS_RANDOM.VALUE(0,0.1));

      IF o.PAY_STAT_ID = 3 THEN
        v_ship_status := 1; -- Cancelled
      ELSIF o.PAY_STAT_ID = 2 THEN
        v_ship_status := TRUNC(DBMS_RANDOM.VALUE(2,4)); -- 2..3
      ELSE
        v_ship_status := 1; -- Pending -> ยังไม่ส่ง (ใช้ Cancelled เป็น placeholder)
      END IF;

      INSERT INTO ORD_DTL
        (ORD_ID, QTY, UNIT_PRICE, DISCOUNT, COMMENT_TEXT, RATING, PRD_ID, SHP_STAT_ID)
      VALUES
        (o.ORD_ID,
         v_qty,
         v_price,
         v_discount,
         CASE WHEN v_ship_status = 3 THEN 'Good quality' ELSE NULL END,
         CASE WHEN v_ship_status = 3 THEN TRUNC(DBMS_RANDOM.VALUE(3,6)) ELSE NULL END,
         v_prd_id,
         v_ship_status);
    END LOOP;
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- 8) CAMPAIGN (1..10) - ผูก PERIOD 1..2
-- =========================================================
DECLARE
  TYPE campaign_array IS VARRAY(20) OF VARCHAR2(100);
  v_campaigns campaign_array := campaign_array(
    'Summer Fruit Festival','Fresh Farm Promotion','Organic Harvest Week','Tropical Fruit Fair',
    'Healthy Veggie Sale','Thai Mango Special','Durian Lover Festival','Weekend Farm Market',
    'Green Season Promotion','Premium Produce Sale','Flash Sale Friday','Farmer Direct Deal',
    'Super Saver Harvest','Rainy Season Discount','Golden Banana Week','Seasonal Fresh Picks',
    'Farm to Table Campaign','Mid-Year Fresh Sale','Vegetable Bonanza','Agro Product Expo'
  );
  v_name VARCHAR2(100);
  v_period NUMBER;
BEGIN
  FOR i IN 1..10 LOOP
    v_name := v_campaigns(TRUNC(DBMS_RANDOM.VALUE(1,21))); -- 1..20
    v_period := TRUNC(DBMS_RANDOM.VALUE(1,3)); -- 1..2

    INSERT INTO CAMPAIGN
      (CMP_ID, NAME, DISCOUNT, START_DATE, END_DATE, PERIOD_ID)
    VALUES
      (i,
       v_name,
       TRUNC(DBMS_RANDOM.VALUE(5,30)),
       SYSDATE - TRUNC(DBMS_RANDOM.VALUE(0,30)),
       SYSDATE + TRUNC(DBMS_RANDOM.VALUE(7,30)),
       v_period);
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- 9) CMP_PRD - ให้ trigger ใส่ SEQ เอง + กันซ้ำ (CMP_ID, PRD_ID)
-- =========================================================
DECLARE
  v_prd_id NUMBER;
  v_item_count NUMBER;
  v_exist NUMBER;
BEGIN
  FOR c IN (SELECT CMP_ID FROM CAMPAIGN WHERE CMP_ID BETWEEN 1 AND 10) LOOP
    v_item_count := TRUNC(DBMS_RANDOM.VALUE(5,16)); -- 5..15

    FOR i IN 1..v_item_count LOOP
      LOOP
        v_prd_id := TRUNC(DBMS_RANDOM.VALUE(1,201)); -- 1..200

        SELECT COUNT(*)
          INTO v_exist
          FROM CMP_PRD
         WHERE CMP_ID = c.CMP_ID
           AND PRD_ID = v_prd_id;

        EXIT WHEN v_exist = 0;
      END LOOP;

      INSERT INTO CMP_PRD (CMP_ID, PRD_ID)
      VALUES (c.CMP_ID, v_prd_id);
    END LOOP;
  END LOOP;
END;
/
COMMIT;

-- =========================================================
-- DONE
-- =========================================================
BEGIN
  DBMS_OUTPUT.PUT_LINE('SEED DONE ✅');
END;
/
