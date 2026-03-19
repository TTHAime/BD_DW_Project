# Agricultural products E-Commerce Data Warehouse

## Overview
 
This project implements an end-to-end Data Warehouse for an Agricultural products  E-Commerce system consisting of:
 
- **Star Schema** on Oracle Database — Fact tables, Dimension tables, and a Bridge table
- **ETL Pipeline** — Extract, Transform, Load via PL/SQL Stored Procedure
- **REST API** — Node.js + Express serving data from Oracle to downstream consumers
- **Dashboard** — Power BI connected through the API, delivering 5 executive reports

 
## Architecture
 
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
