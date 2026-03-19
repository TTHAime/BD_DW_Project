# Agricultural products E-Commerce Data Warehouse

## Overview
 
This project implements an end-to-end Data Warehouse for an Agricultural products  E-Commerce system consisting of:
 
- **Star Schema** on Oracle Database — Fact tables, Dimension tables, and a Bridge table
- **ETL Pipeline** — Extract, Transform, Load via PL/SQL Stored Procedure
- **REST API** — Node.js + Express serving data from Oracle to downstream consumers
- **Dashboard** — Power BI connected through the API, delivering 5 executive reports

---
 
## Tech Stack
 
| Technology | Version | Role |
|------------|---------|------|
| Oracle Database | 26 | Data Warehouse storage and ETL execution |
| PL/SQL | — | ETL procedures and triggers |
| Node.js | 18+ | REST API runtime |
| Express.js | 4.x | HTTP routing framework |
| node-oracledb | 6.x | Oracle Database connector for Node.js |
| Power BI | Desktop | Dashboard and data visualization |
| Docker | Desktop | Container host for Oracle Database |
 
---
 
## Architecture
 
```
┌──────────────┐     ┌──────────────┐     ┌────────────────┐
│  Source DB   │────▶│   Staging    │────▶│ Data Warehouse │
│   (OLTP)     │ ETL │   (STG_*)    │Clean│    (DW_*)      │
└──────────────┘     └──────────────┘     └────────────────┘
     database/            database/              database/
       seed/                etl/                 + views/
                                                      │      Direct Connect
                                 ┌────────────────────┴────────────────────┐
                                 │                                         │
                                 ▼                                         ▼
                        ┌──────────────┐                          ┌──────────────┐
                        │   REST API   │                          │   Power BI   │
                        │  server.js   │                          │  Dashboard   │
                        └──────────────┘                          └──────────────┘
                                 │                                         ▲
                                 └────────────────────▶────────────────────┘
                                                    API 
```

## Star Schema
 
```
                    ┌─────────────────┐
                    │  DW_DIM_DATE    │
                    │  DATE_KEY (PK)  │
                    │  YEAR, QUARTER  │
                    │  MONTH, WEEK    │
                    └────────┬────────┘
                             │
┌─────────────────┐          │          ┌─────────────────┐
│ DW_DIM_PRODUCT  │          │          │  DW_DIM_SHOP    │
│ PRD_KEY (PK)    │──┐       │       ┌──│  SHOP_KEY (PK)  │
│ PRD_NAME, PRICE │  │       │       │  │  SHOP_NAME      │
│ STOCK, DISCOUNT │  │       │       │  │  RATING_AVG     │
└─────────────────┘  │       │       │  └─────────────────┘
                     ▼       ▼       ▼
               ┌────────────────────────────┐
               │   DW_FACT_ORDER_LINE       │
               │   ORD_ID + SEQ (PK)        │
               │   ORDER_DATE_KEY (FK)      │
               │   PRD_KEY, SHOP_KEY (FK)   │
               │   QTY, UNIT_PRICE          │
               │   DISCOUNT, LINE_AMOUNT    │
               │   RATING, COMMENT_TEXT     │
               └──────────┬─────────────────┘
                     │       │       │
                     ▼       ▼       ▼
┌─────────────────┐  │       │       │  ┌─────────────────┐
│ DW_DIM_CATEGORY │──┘       │       └──│ DW_DIM_SHP_STAT │
│ CAT_ID (PK)     │          │          │ SHP_STAT_ID(PK) │
└─────────────────┘ ┌────────┴────────┐ └─────────────────┘
                    │ DW_DIM_PAY_STAT │
                    │ PAY_STAT_ID(PK) │
                    └─────────────────┘
 
Bridge: DW_BRIDGE_CAMPAIGN_PRODUCT (CMP_ID ↔ PRD_ID)
```

## Project Structure
 
```
📦 e-commerce-data-warehouse/
│
├── 📁 database/                        # All SQL scripts
│   │
│   ├── 📁 ddl/                         # Data Definition Language
│   │   ├── 00_drop_all.sql             #   Drop all tables, sequences, procedures
│   │   └── 01_create_tables.sql        #   CREATE TABLE, SEQUENCE, INDEX
│   │
│   ├── 📁 etl/                         # Extract-Transform-Load
│   │   └── pr_etl_full_refresh.sql     #   Stored procedure for full refresh ETL
│   │
│   ├── 📁 seed/                        # Data population
│   │   ├── 01_seed_lookup.sql          #   Seed lookup tables (CATEGORY, PRD_TYPE, ...)
│   │   ├── 02_insert_full.sql          #   Full dataset insert
│   │   ├── 03_insert_mini.sql          #   Small sample dataset (for testing)
│   │   └── 📁 procedures/              #   Additional stored procedures
│   │
│   ├── 📁 views/                       # Database views
│   │   └── dashboard_views.sql         #   ORDER_LINE_FLAT, VW_DASH_CAMPAIGN_PRODUCT
│   │
│   └── 📁 triggers/                    # Triggers
│       └── triggers.sql                #   Database trigger scripts
│
├── 📁 api/                             # REST API (Node.js)
│   ├── server.js                       #   Express server + Oracle connection
│   ├── package.json                    #   npm dependencies
│   └── .env                            #   Environment variables (not in git)
│
├── .gitignore
└── README.md
```
