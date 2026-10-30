# Olist E-Commerce — An End-to-End ELT Pipeline

An end-to-end ELT data pipeline built with **Databricks, Python (PySpark), and SQL**, using the Brazilian Olist E-Commerce dataset. The project implements a Medallion Architecture (Bronze, Silver, and Gold) to ingest, transform, clean, and model raw e-commerce data into analytics-ready datasets.

## Project Overview

The goal of this project is to build a scalable analytical data pipeline that transforms raw e-commerce data into structured datasets for business intelligence and analytical queries.

The pipeline processes data from multiple sources and organizes it into three architectural layers:

- **Bronze:** Raw data ingestion and initial storage.
- **Silver:** Data cleaning, standardization, and transformation.
- **Gold:** Business-oriented aggregations, analytical dimensions, and fact tables.

## Architecture

```text
                 Data Sources
                      |
                      v
              Supabase Storage
                 (S3 API)
                      |
                      v
               BRONZE LAYER
              Raw Data Ingestion
                      |
                      v
               SILVER LAYER
          Data Cleaning & Transformation
                      |
                      v
                GOLD LAYER
          Analytical Data Modeling
                      |
                      v
             Analytics & Reporting
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Databricks | Data engineering platform |
| Python | Data ingestion and processing |
| PySpark | Distributed data transformations |
| SQL | Data modeling and analytical queries |
| Delta Lake | Structured data storage |
| Unity Catalog | Data organization and governance |
| boto3 | S3-compatible object storage access |
| Supabase Storage | Source data storage |

## Data Source

The project uses the **Brazilian E-Commerce Public Dataset by Olist**, containing information about orders, customers, products, sellers, payments, reviews.

Main datasets include:

- Customers
- Orders
- Order Items
- Payments
- Products
- Sellers
- Reviews
- Product Category Translation

Source: [Olist Brazilian E-Commerce Dataset — Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## Data Layers

### Bronze

Responsible for ingesting and storing the source datasets in their initial form. The ingestion process integrates with Supabase Storage through its S3-compatible API, using Python and boto3.

### Silver

Responsible for preparing the data for analytical use through:

- Data cleaning and standardization.
- Handling missing and inconsistent values.
- Data type corrections.
- Business-rule implementation.
- Data quality improvements.

Transformations are implemented using Python and PySpark.

### Gold

Contains business-oriented datasets designed to support analytical queries and reporting.

Current analytical models include:

| Dataset | Grain | Description |
|---|---|---|
| `full_order` | One row per order | Order-level metrics enriched with customer, item, payment, and review information |
| `full_logistics` | Logistics analytical grain | Logistics-related metrics and attributes |
| `dim_customers` | One row per customer | Customer behavior, spending, location, and review metrics |
| `dim_products` | One row per product | Product attributes, sales metrics, and review information |
| `dim_sellers` | One row per seller | Seller-level analytical attributes and metrics |

Gold transformations use SQL and Databricks Materialized Views.

## Repository Structure

```text
olist-end-to-end/
│
├── transformations/
│   │
│   ├── Ingestion.py
│   │
│   ├── silver/
│   │   ├── customers.py
│   │   ├── order_items.py
│   │   ├── orders.py
│   │   ├── payments.py
│   │   ├── products.py
│   │   ├── reviews.py
│   │   └── sellers.py
│   │
│   └── gold/
│       ├── dim_customers.sql
│       ├── dim_products.sql
│       ├── dim_sellers.sql
│       ├── full_logistics.sql
│       └── full_order.sql
│
├── .gitignore
├── LICENSE
└── README.md
```

## Key Engineering Concepts

This project explores and applies several data engineering concepts:

- Medallion Architecture.
- ELT pipeline design.
- Distributed data processing with PySpark.
- SQL-based analytical transformations.
- Delta Lake tables.
- Data modeling and dimensional modeling.
- Data quality and consistency checks.
- Aggregations and window functions.
- Materialized Views.
- Cloud object storage integration.
- Git-based version control.

---

**Author:** [tamakkj](https://github.com/tamakkj)
