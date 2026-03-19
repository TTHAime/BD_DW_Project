-- 1) Insert ORDER (เลือก customer 2..21, pay_stat ใช้ 2=Paid ตาม seed) :contentReference[oaicite:1]{index=1}
INSERT INTO ORDERS
  (ORD_ID, ORDER_DATE, TOTAL_AMOUNT, TOTAL_DISCOUNT, USR_ID, PAY_STAT_ID)
VALUES
  (202, SYSDATE, 999.00, 50.00, 2, 2);

-- 2) Insert 2 ORDER LINES (ปล่อยให้ TRG_ORD_DTL_SEQ ใส่ SEQ เอง) :contentReference[oaicite:2]{index=2}
-- ใช้ PRD_ID ในช่วง 1..200 (seed สร้างสินค้า 1..200)
INSERT INTO ORD_DTL
  (ORD_ID, QTY, UNIT_PRICE, DISCOUNT, COMMENT_TEXT, RATING, PRD_ID, SHP_STAT_ID)
VALUES
  (202, 2, 120.00, 10.00, 'ZZ_BI_AFTERSEED_202_LINE_1', 5, 1, 3); -- Delivered

INSERT INTO ORD_DTL
  (ORD_ID, QTY, UNIT_PRICE, DISCOUNT, COMMENT_TEXT, RATING, PRD_ID, SHP_STAT_ID)
VALUES
  (202, 1, 300.00, 0.00, 'ZZ_BI_AFTERSEED_202_LINE_2', 4, 2, 3); -- Delivered

COMMIT;

BEGIN
  PR_ETL_FULL_REFRESH;
END;
