const express = require("express");
const cors = require("cors");
const oracledb = require("oracledb");

const app = express();
app.use(cors());
app.use(express.json());

oracledb.outFormat = oracledb.OUT_FORMAT_OBJECT;
oracledb.fetchAsString = [oracledb.CLOB];

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

// =========================================================
// Health & DB Test
// =========================================================
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

// =========================================================
// ETL
// =========================================================
app.post("/etl/run", async (req, res) => {
  try {
    await withConn(async (conn) => {
      await conn.execute(`BEGIN PR_ETL_FULL_REFRESH; END;`);
    });
    res.json({ ok: true, message: "ETL completed" });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// =========================================================
// OLTP: Create Order
// =========================================================
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
      const exists = await conn.execute(
        `SELECT 1 FROM ORDERS WHERE ORD_ID = :id`,
        { id: Number(ord_id) },
      );
      if (exists.rows.length > 0) return { conflict: true };

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

      const lineSql = `
        INSERT INTO ORD_DTL
          (ORD_ID, SEQ, QTY, UNIT_PRICE, DISCOUNT, COMMENT_TEXT, RATING, PRD_ID, SHP_STAT_ID)
        VALUES
          (:ord_id, :seq, :qty, :unit_price, :discount, :comment_text, :rating, :prd_id, :shp_stat_id)
      `;
      const binds = lines.map((l) => ({
        ord_id: Number(ord_id),
        seq: l.seq == null ? null : Number(l.seq),
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

// =========================================================
// DW VIEWS — ดึงจาก View ที่สร้างไว้ใน DB โดยตรง
// =========================================================

/**
 * GET /api/order-line-flat
 * Source: VIEW ORDER_LINE_FLAT (Fact + all Dims joined)
 * Used by: Report 1-5
 */
app.get("/api/order-line-flat", async (req, res) => {
  const limit = toNum(req.query.limit ?? 50000, 50000);
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `SELECT * FROM ORDER_LINE_FLAT WHERE ROWNUM <= :limit`,
        { limit },
      );
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

/**
 * GET /api/campaign-product
 * Source: VIEW VW_DASH_CAMPAIGN_PRODUCT (Bridge + Campaign + Product)
 * Used by: Report 3 (Campaign Effectiveness)
 */
app.get("/api/campaign-product", async (req, res) => {
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(`SELECT * FROM VW_DASH_CAMPAIGN_PRODUCT`);
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// =========================================================
// DW DIMENSIONS — ดึงจากตาราง Dim โดยตรง
// =========================================================

/**
 * GET /api/dim/date
 * Source: DW_DIM_DATE
 */
app.get("/api/dim/date", async (req, res) => {
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `SELECT * FROM DW_DIM_DATE ORDER BY DATE_KEY`,
      );
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

/**
 * GET /api/dim/product
 * Source: VW_DIM_PRODUCT_CURRENT (IS_CURRENT = Y only)
 * Use /api/dim/product?all=true for full SCD2 history
 */
app.get("/api/dim/product", async (req, res) => {
  const showAll = req.query.all === "true";
  try {
    const rows = await withConn(async (conn) => {
      const sql = showAll
        ? `SELECT * FROM DW_DIM_PRODUCT ORDER BY PRD_ID, EFFECTIVE_DATE`
        : `SELECT * FROM VW_DIM_PRODUCT_CURRENT ORDER BY PRD_ID`;
      const r = await conn.execute(sql);
      return r.rows;
    });
    res.json({
      status: "success",
      count: rows.length,
      scd2_history: showAll,
      data: rows,
    });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

/**
 * GET /api/dim/shop
 * Source: VW_DIM_SHOP_CURRENT (IS_CURRENT = Y only)
 * Use /api/dim/shop?all=true for full SCD2 history
 */
app.get("/api/dim/shop", async (req, res) => {
  const showAll = req.query.all === "true";
  try {
    const rows = await withConn(async (conn) => {
      const sql = showAll
        ? `SELECT * FROM DW_DIM_SHOP ORDER BY SHOP_ID, EFFECTIVE_DATE`
        : `SELECT * FROM VW_DIM_SHOP_CURRENT ORDER BY SHOP_ID`;
      const r = await conn.execute(sql);
      return r.rows;
    });
    res.json({
      status: "success",
      count: rows.length,
      scd2_history: showAll,
      data: rows,
    });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

/**
 * GET /api/dim/campaign
 * Source: DW_DIM_CAMPAIGN (IS_CURRENT = Y only)
 * Use /api/dim/campaign?all=true for full SCD2 history
 */
app.get("/api/dim/campaign", async (req, res) => {
  const showAll = req.query.all === "true";
  try {
    const rows = await withConn(async (conn) => {
      const sql = showAll
        ? `SELECT * FROM DW_DIM_CAMPAIGN ORDER BY CMP_ID, EFFECTIVE_DATE`
        : `SELECT * FROM DW_DIM_CAMPAIGN WHERE IS_CURRENT = 'Y' ORDER BY CMP_ID`;
      const r = await conn.execute(sql);
      return r.rows;
    });
    res.json({
      status: "success",
      count: rows.length,
      scd2_history: showAll,
      data: rows,
    });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// =========================================================
// DW FACTS — ดึงจากตาราง Fact โดยตรง
// =========================================================

/**
 * GET /api/fact/orders
 * Source: DW_FACT_ORDER
 */
app.get("/api/fact/orders", async (req, res) => {
  const limit = toNum(req.query.limit ?? 50000, 50000);
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `SELECT * FROM DW_FACT_ORDER WHERE ROWNUM <= :limit ORDER BY ORD_ID`,
        { limit },
      );
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

/**
 * GET /api/fact/order-lines
 * Source: DW_FACT_ORDER_LINE
 */
app.get("/api/fact/order-lines", async (req, res) => {
  const limit = toNum(req.query.limit ?? 50000, 50000);
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `SELECT * FROM DW_FACT_ORDER_LINE WHERE ROWNUM <= :limit ORDER BY ORD_ID, SEQ`,
        { limit },
      );
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// =========================================================
// Legacy endpoints (redirect to new paths)
// =========================================================
app.get("/pbi/order_line_flat", (req, res) =>
  res.redirect(301, "/api/order-line-flat" + (req._parsedUrl.search || "")),
);
app.get("/pbi/sales_daily", async (req, res) => {
  const days = toNum(req.query.days ?? 180, 180);
  try {
    const rows = await withConn(async (conn) => {
      const r = await conn.execute(
        `
        SELECT
          ORDER_DATE_KEY,
          SUM(LINE_AMOUNT)    AS TOTAL_REVENUE,
          SUM(LINE_DISCOUNT)  AS TOTAL_DISCOUNT,
          SUM(QTY)            AS TOTAL_QTY,
          COUNT(DISTINCT ORD_ID) AS ORDER_COUNT
        FROM ORDER_LINE_FLAT
        WHERE DATE_VALUE >= TRUNC(SYSDATE) - :days
        GROUP BY ORDER_DATE_KEY
        ORDER BY ORDER_DATE_KEY
      `,
        { days },
      );
      return r.rows;
    });
    res.json({ status: "success", count: rows.length, data: rows });
  } catch (err) {
    res.status(500).json({ status: "error", error: err.message });
  }
});

// =========================================================
const port = process.env.PORT || 3000;
app.listen(port, () => {
  console.log(`API running on :${port}`);
  console.log(`Endpoints:`);
  console.log(`  GET  /health`);
  console.log(`  GET  /db-test`);
  console.log(`  POST /etl/run`);
  console.log(`  POST /orders/raw`);
  console.log(`  GET  /api/order-line-flat`);
  console.log(`  GET  /api/campaign-product`);
  console.log(`  GET  /api/dim/date`);
  console.log(`  GET  /api/dim/product       (?all=true for SCD2 history)`);
  console.log(`  GET  /api/dim/shop          (?all=true for SCD2 history)`);
  console.log(`  GET  /api/dim/campaign      (?all=true for SCD2 history)`);
  console.log(`  GET  /api/fact/orders`);
  console.log(`  GET  /api/fact/order-lines`);
  console.log(`  GET  /pbi/sales_daily`);
});
