const express = require("express");
const cors = require("cors");
const oracledb = require("oracledb");

const app = express();
app.use(cors());
app.use(express.json());

// Return rows as objects (key = column alias)
oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;

// Optional: fetch DATE/TIMESTAMP as JS Date (default is OK too)
oracledb.fetchAsString = [oracledb.CLOB]; // if you ever have CLOB
// oracledb.fetchAsBuffer = []; // not needed

const ORACLE_USER = process.env.ORACLE_USER || "ADMINDB";
const ORACLE_PASS = process.env.ORACLE_PASS || "sql123";
const ORACLE_HOST = process.env.ORACLE_HOST || "localhost";
const ORACLE_PORT = process.env.ORACLE_PORT || "1521";
const ORACLE_SERVICE = process.env.ORACLE_SERVICE || "FREEPDB1";

const connectString = `//${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}`;

function toNum(v, def = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

async function withConn(fn) {
  let conn;
  try {
    conn = await oracledb.getConnection({
      user: ORACLE_USER,
      password: ORACLE_PASS,
      connectString,
    });
    return await fn(conn);
  } finally {
    if (conn) {
      try {
        await conn.close();
      } catch {}
    }
  }
}

async function runETL() {
  return withConn(async (conn) => {
    // PR_ETL_FULL_REFRESH does its own COMMIT/ROLLBACK already
    await conn.execute(`BEGIN PR_ETL_FULL_REFRESH; END;`);
    return true;
  });
}

function calcTotals(lines) {
  let totalAmount = 0;
  let totalDiscount = 0;
  for (const l of lines) {
    const qty = Number(l.qty ?? 0);
    const unit = Number(l.unit_price ?? 0);
    const disc = Number(l.discount ?? 0);
    totalAmount += qty * unit;
    totalDiscount += disc;
  }
  return {
    totalAmount: Math.round(totalAmount * 100) / 100,
    totalDiscount: Math.round(totalDiscount * 100) / 100,
  };
}

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/db-test", async (req, res) => {
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `SELECT USER AS U, sys_context('USERENV','CON_NAME') AS PDB FROM dual`,
      );
      return r.rows;
    });
    res.json({ ok: true, rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/etl/run", async (req, res) => {
  try {
    await runETL();
    res.json({ ok: true, message: "ETL completed" });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

/**
 * Create raw OLTP order (ORDERS + ORD_DTL)
 * - Compatible with your OLTP tables + TRG_ORD_DTL_SEQ
 * - IMPORTANT: we DO NOT require "seq" in request; trigger will generate it if missing
 *
 * Body:
 * {
 *   "ord_id": 123,
 *   "order_date": "2026-02-25T12:00:00Z" (optional),
 *   "usr_id": 10,
 *   "pay_stat_id": 2,
 *   "total_amount": 1234.56 (optional, else computed),
 *   "total_discount": 12.34 (optional, else computed),
 *   "lines": [
 *     { "prd_id": 5, "qty": 2, "unit_price": 99.9, "discount": 0, "shp_stat_id": 2, "comment_text": null, "rating": null }
 *   ]
 * }
 */
app.post("/orders/raw", async (req, res) => {
  const body = req.body || {};
  const { ord_id, order_date, usr_id, pay_stat_id, lines } = body;

  if (
    ord_id == null ||
    usr_id == null ||
    pay_stat_id == null ||
    !Array.isArray(lines) ||
    lines.length === 0
  ) {
    return res.status(400).json({
      ok: false,
      error: "ord_id, usr_id, pay_stat_id, lines[] are required",
    });
  }

  for (const [i, l] of lines.entries()) {
    // seq NOT required
    if (!l.prd_id || l.qty == null || l.unit_price == null || !l.shp_stat_id) {
      return res.status(400).json({
        ok: false,
        error: `lines[${i}] must have prd_id, qty, unit_price, shp_stat_id`,
      });
    }
  }

  const computed = calcTotals(lines);
  const totalAmount =
    body.total_amount != null
      ? Number(body.total_amount)
      : computed.totalAmount;
  const totalDiscount =
    body.total_discount != null
      ? Number(body.total_discount)
      : computed.totalDiscount;

  try {
    const result = await withConn(async (conn) => {
      // Use a transaction
      // 1) prevent duplicate ORD_ID
      const exists = await conn.execute(
        `SELECT 1 FROM ORDERS WHERE ORD_ID = :id`,
        { id: Number(ord_id) },
      );
      if (exists.rows.length > 0) return { conflict: true };

      // 2) insert ORDERS
      await conn.execute(
        `INSERT INTO ORDERS (ORD_ID, ORDER_DATE, TOTAL_AMOUNT, TOTAL_DISCOUNT, USR_ID, PAY_STAT_ID)
         VALUES (:ord_id, :order_date, :total_amount, :total_discount, :usr_id, :pay_stat_id)`,
        {
          ord_id: Number(ord_id),
          order_date: order_date ? new Date(order_date) : new Date(),
          total_amount: totalAmount,
          total_discount: totalDiscount,
          usr_id: Number(usr_id),
          pay_stat_id: Number(pay_stat_id),
        },
        { autoCommit: false },
      );

      // 3) insert ORD_DTL
      // NOTE: allow SEQ to be NULL -> trigger TRG_ORD_DTL_SEQ will set it
      const lineSql = `
        INSERT INTO ORD_DTL
          (ORD_ID, SEQ, QTY, UNIT_PRICE, DISCOUNT, COMMENT_TEXT, RATING, PRD_ID, SHP_STAT_ID)
        VALUES
          (:ord_id, :seq, :qty, :unit_price, :discount, :comment_text, :rating, :prd_id, :shp_stat_id)
      `;

      const binds = lines.map((l) => ({
        ord_id: Number(ord_id),
        seq: l.seq == null ? null : Number(l.seq), // ✅ null allowed => trigger fills
        qty: Number(l.qty),
        unit_price: Number(l.unit_price),
        discount: l.discount == null ? 0 : Number(l.discount),
        comment_text: l.comment_text ?? null,
        rating: l.rating ?? null,
        prd_id: Number(l.prd_id),
        shp_stat_id: Number(l.shp_stat_id),
      }));

      await conn.executeMany(lineSql, binds, { autoCommit: false });

      await conn.commit();
      return { conflict: false };
    });

    if (result.conflict) {
      return res
        .status(409)
        .json({ ok: false, error: `ORD_ID ${ord_id} already exists` });
    }

    res.json({
      ok: true,
      ord_id: Number(ord_id),
      total_amount: totalAmount,
      total_discount: totalDiscount,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

/**
 * Flat table for Power BI
 * Uses your DW tables from PR_ETL_FULL_REFRESH:
 * - DW_FACT_ORDER_LINE l
 * - DW_FACT_ORDER o
 * - DW_DIM_SHOP s
 * - DW_DIM_PRODUCT p
 * - Lookup dims: DW_DIM_CATEGORY c, DW_DIM_PRD_TYPE pt, DW_DIM_PAY_STAT ps, DW_DIM_SHP_STAT ss
 */
app.get("/pbi/order_line_flat", async (req, res) => {
  const days = toNum(req.query.days ?? 90, 90);
  const limit = toNum(req.query.limit ?? 50000, 50000);

  const sql = `
    SELECT *
    FROM (
      SELECT
        l.ORD_ID,
        l.SEQ,

        l.ORDER_DATE,
        TRUNC(l.ORDER_DATE) AS ORDER_DAY,      -- ✅ ใช้ทำกราฟรายวันง่าย

        l.USR_ID,

        l.SHP_STAT_ID,
        ss.NAME AS SHP_STAT_NAME,

        l.QTY,
        l.UNIT_PRICE,

        -- ✅ คำนวณยอดรายบรรทัดให้เสร็จ
        (NVL(l.QTY,0) * NVL(l.UNIT_PRICE,0)) AS GROSS_LINE_AMOUNT,
        NVL(l.DISCOUNT,0) AS LINE_DISCOUNT,
        ((NVL(l.QTY,0) * NVL(l.UNIT_PRICE,0)) - NVL(l.DISCOUNT,0)) AS NET_LINE_AMOUNT,

        l.LINE_AMOUNT,                        
        l.RATING,
        l.COMMENT_TEXT,

        s.SHOP_KEY,
        s.SHOP_ID,
        s.SHOP_NAME,
        s.RATING_AVG,

        p.PRD_KEY,
        p.PRD_ID,
        p.PRD_NAME,
        p.DESCRIPTION,

        p.CAT_ID,
        c.NAME AS CAT_NAME,                    -- ✅ CAT_NAME มาจาก DW_DIM_CATEGORY.NAME

        p.PRD_TYPE_ID,
        pt.NAME AS PRD_TYPE_NAME,

        p.PRICE AS LIST_PRICE,
        p.DISCOUNT AS PRD_DISCOUNT,

        o.PAY_STAT_ID,
        ps.NAME AS PAY_STAT_NAME,

        o.TOTAL_AMOUNT,
        o.TOTAL_DISCOUNT

      FROM DW_FACT_ORDER_LINE l
      JOIN DW_FACT_ORDER o
        ON o.ORD_ID = l.ORD_ID

      LEFT JOIN DW_DIM_SHOP s
        ON s.SHOP_KEY = l.SHOP_KEY

      LEFT JOIN DW_DIM_PRODUCT p
        ON p.PRD_KEY = l.PRD_KEY

      LEFT JOIN DW_DIM_CATEGORY c
        ON c.CAT_ID = p.CAT_ID

      LEFT JOIN DW_DIM_PRD_TYPE pt
        ON pt.PRD_TYPE_ID = p.PRD_TYPE_ID

      LEFT JOIN DW_DIM_PAY_STAT ps
        ON ps.PAY_STAT_ID = o.PAY_STAT_ID

      LEFT JOIN DW_DIM_SHP_STAT ss
        ON ss.SHP_STAT_ID = l.SHP_STAT_ID

      WHERE l.ORDER_DATE >= TRUNC(SYSDATE) - :days
      ORDER BY l.ORDER_DATE DESC, l.ORD_ID DESC, l.SEQ
    )
    WHERE ROWNUM <= :limit
  `;

  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(sql, { days, limit });
      return r.rows;
    });

    const data = rows.map((x) => ({
      ...x,
      ORDER_DATE: x.ORDER_DATE ? new Date(x.ORDER_DATE).toISOString() : null,
      ORDER_DAY: x.ORDER_DAY
        ? new Date(x.ORDER_DAY).toISOString().slice(0, 10)
        : null,
    }));

    res.json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

/**
 * Daily Sales (from DW_FACT_ORDER)
 */
app.get("/pbi/sales_daily", async (req, res) => {
  const days = toNum(req.query.days ?? 180, 180);

  const sql = `
    SELECT
      TRUNC(ORDER_DATE) AS ORDER_DAY,
      SUM(TOTAL_AMOUNT) AS GROSS_AMOUNT,
      SUM(TOTAL_DISCOUNT) AS TOTAL_DISCOUNT,
      SUM(TOTAL_AMOUNT - TOTAL_DISCOUNT) AS NET_AMOUNT,
      COUNT(*) AS ORDER_COUNT
    FROM DW_FACT_ORDER
    WHERE ORDER_DATE >= TRUNC(SYSDATE) - :days
    GROUP BY TRUNC(ORDER_DATE)
    ORDER BY ORDER_DAY
  `;

  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(sql, { days });
      return r.rows;
    });

    const data = rows.map((x) => ({
      ...x,
      ORDER_DAY: x.ORDER_DAY
        ? new Date(x.ORDER_DAY).toISOString().slice(0, 10)
        : null,
    }));

    res.json(data);
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

const port = process.env.PORT || 3000;
app.listen(port, () => console.log(`API running on :${port}`));
