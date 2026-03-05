# 🏪 Sales Data Warehouse — End-to-End ETL Pipeline

A complete, production-ready **Data Warehouse** built from scratch using **Medallion Architecture** (Bronze → Silver → Gold), powered by **Python** and **SQL Server**, ultimately powering interactive **Power BI** dashboards.

---

## 📐 Architecture Overview

The project follows the **Medallion (Lakehouse) Architecture** pattern:

| Layer | Schema | Object Type | Purpose |
|-------|--------|-------------|---------|
| **Bronze** | bronze | Table | Raw data — all columns NVARCHAR, no transformations |
| **Silver** | silver | Table + SP | Cleaned, typed, deduplicated data |
| **Gold** | gold | Views + Table | Star Schema — ready for BI consumption |

---

## 📁 Repository Structure
```
sales-data-warehouse/
│
├── bronze/
│   └── load.ipynb
│
├── silver/
│   └── DDL_For_SilverLayer.sql
    └── SilverStoredProcedure.sql
│
└── gold/
└── GoldLayer.sql
```
---

## ⚙️ Bronze Layer — Python ETL

| Feature | Description |
|---------|-------------|
| **Dual Load Strategy** | Bulk Load for historical files — Incremental Load for future new files |
| **Duplicate Prevention** | `loaded_files` audit table prevents loading the same file twice |
| **Batch Processing** | Data inserted in batches of 1,000 rows to avoid memory issues |
| **Date Intelligence** | Incremental mode loads only rows newer than the latest `order_date` in the database |
| **Audit Trail** | Each row carries a `source_file` column for full lineage tracking |

**Load Results:**

| File | Rows | Load Type |
|------|------|-----------|
| Sales 2022.csv | 1,993 | Bulk |
| Sales 2023.csv | 2,102 | Bulk |
| Sales 2024.csv | 2,587 | Bulk |
| Sales 2025.csv | 3,312 | Bulk |
| **Total** | **9,994** | |

---

## 🧹 Silver Layer — Transformation & Cleansing

| Transformation | Detail |
|----------------|--------|
| **Data Type Casting** | `NVARCHAR` → `INT`, `DECIMAL(10,2)`, `DATE` |
| **Date Normalization** | 6 formats: DD/MM/YYYY, MM/DD/YYYY, YYYY-MM-DD, D/M/YYYY, M/D/YYYY, Excel Serial Numbers |
| **Category Standardization** | Segment, Region, Ship Mode normalized to consistent casing |
| **Numeric Validation** | Sales, Profit, Discount, Quantity validated for correct ranges |
| **Deduplication** | `ROW_NUMBER() OVER (PARTITION BY order_id)` keeps most recent version |

---

## 🗂️ Gold Layer — Star Schema

| Object | Type | Key | Description |
|--------|------|-----|-------------|
| `dim_customers` | View | Customer_key | Unique customers with segment classification |
| `dim_products` | View | product_key | Product catalog with category and sub-category |
| `dim_locations` | View | loc_key | Geographic hierarchy: city, state, country, region |
| `dim_dates` | Table | date_key (INT) | Date dimension 2020–2030 with day, month, quarter, year |
| `fact_sales` | View | order_id | Transactional fact table joining all four dimensions |

---

## 🔄 Data Flow

| Step | Process | Output |
|------|---------|--------|
| 1 | Python reads CSV files from source folder | Raw rows ready for insert |
| 2 | Audit check: has this file been loaded before? | Skip if yes / proceed if no |
| 3 | Batch insert into bronze.All_Raw_sales | 9,994 rows in Bronze |
| 4 | Execute Silver Stored Procedure | Cleaned rows in silver.AllSales |
| 5 | Gold Views auto-refresh on query | Star Schema served to Power BI |
| 6 | Power BI connects via SQL Server connector | Interactive dashboards |

---

## 📊 Power BI Integration

**Connection:** SQL Server → Import Mode → gold schema

**Relationships:**
- `fact_sales[date_key]` → `dim_dates[date_key]`
- `fact_sales[customer_key]` → `dim_customers[Customer_key]`
- `fact_sales[Product_key]` → `dim_products[product_key]`
- `fact_sales[Location_key]` → `dim_locations[loc_key]`

---

## ✅ Data Quality & Validation

| Check | Method | Layer |
|-------|--------|-------|
| No duplicate orders | `ROW_NUMBER() PARTITION BY order_id` | Silver |
| Valid date formats | `TRY_CONVERT` with fallback for 6 formats | Silver |
| No null surrogate keys | WHERE clause filters on all dimension views | Gold |
| Referential integrity | LEFT JOINs expose unmatched keys as NULL | Gold |
| Date dimension coverage | dim_dates spans 2020–2030 (3,287+ rows) | Gold |

---

## 🔐 Security & Access Control

| Step | Command | Scope |
|------|---------|-------|
| 1 | `CREATE LOGIN` | master |
| 2 | `CREATE USER` | SalesDWHP |
| 3 | `CREATE ROLE SeniorDataAnalyst` | SalesDWHP |
| 4 | `GRANT SELECT` on all Gold Views | SalesDWHP |
| 5 | `ALTER ROLE ... ADD MEMBER` | SalesDWHP |

---

## 🚀 How to Run

1. Open `bronze/load.ipynb` — update `SERVER`, `DATABASE`, `CSV_FOLDER` — run all cells
2. Run `silver/DDL_For_SilverLayer.sql , silver/SilverStoredProcedure.sql` in SSMS
3. Run `gold/GoldLayer.sql` in SSMS
4. Connect Power BI → SQL Server → Import Mode → select all 5 gold tables
