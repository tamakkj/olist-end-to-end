# Olist E-Commerce | End-to-End Data Pipeline

An end-to-end ELT pipeline built with **Databricks, PySpark, and SQL**, transforming raw Brazilian e-commerce data into structured analytical datasets through the Medallion Architecture.

The project covers data ingestion, transformation, data quality, and analytical modeling, using Databricks Lakeflow Spark Declarative Pipelines and SQL Materialized Views.

## Architecture

The pipeline follows the Medallion Architecture, separating data processing into three layers:

```text
Supabase Storage (S3-compatible API)
                |
                v
        BRONZE LAYER
     Raw Data Ingestion
     Python | boto3 | pandas
                |
                v
         SILVER LAYER
   Cleaning & Standardization
   PySpark | Data Quality
                |
                v
          GOLD LAYER
      Analytical Data Models
       SQL | Materialized Views
                |
                v
       Analytics & Reporting
```

### Bronze — Data Ingestion

The ingestion layer retrieves CSV files from Supabase Storage through its S3-compatible API.

Implementation highlights:
- Python integration using boto3.
- CSV retrieval and parsing with pandas.
- Conversion into Spark DataFrames.
- Publication of raw datasets as Bronze materialized views.

### Silver — Data Transformation

The Silver layer prepares the ingested data for analytical consumption using PySpark.

Transformations include:
- Data type casting and timestamp parsing.
- Standardization of customer and seller locations.
- Product category translation and completeness checks.
- Review deduplication and text normalization.
- Payment and shipping validation.
- Business-rule implementation and data quality flags.

Examples of implemented quality indicators:
- Inconsistent customer locations across orders.
- Missing order approval timestamps.
- Invalid delivery date sequences.
- Late deliveries.
- Incomplete product records.
- Shipping and payment inconsistencies.

### Gold — Analytical Modeling

The Gold layer implements business-oriented datasets using SQL Materialized Views.

Each model has a defined grain and analytical purpose.

| Model | Grain | Purpose |
|---|---|---|
| `full_order` | One row per order | Order value, payment methods, purchasing behavior and customer reviews |
| `full_logistics` | One row per delivered order | Delivery performance, delays and regional logistics |
| `dim_customers` | One row per unique customer | Customer spending, repeat purchases and geographic distribution |
| `dim_products` | One row per product | Product revenue, sales volume, categories and reviews |
| `dim_sellers` | One row per seller | Seller revenue, sales performance and delivery metrics |

The models incorporate aggregated metrics from multiple Silver datasets, with business rules applied according to each analytical requirement.

## Technology Stack

| Technology | Application |
|---|---|
| Databricks | Data engineering environment |
| Python | Ingestion and data processing |
| PySpark | Distributed data transformations |
| SQL | Analytical modeling and aggregations |
| Lakeflow Spark Declarative Pipelines | Pipeline transformations and materialized views |
| Delta Lake | Data storage |
| Supabase Storage | Source data storage |
| boto3 | S3-compatible API integration |
| pandas | CSV parsing during ingestion |
| Git & GitHub | Source code versioning |

## Data Source

The project uses the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce).

The dataset contains approximately 100,000 orders and relational information covering:

- Customers
- Orders
- Order items
- Payments
- Products
- Sellers
- Reviews
- Product category translations

The source CSV files are hosted in Supabase Storage and accessed through its S3-compatible API.

## Repository Structure

```text
olist-end-to-end/
│
├── transformations/
│   ├── bronze/
│   │   └── Ingestion.py
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

## Execution Environment

The transformations are developed for execution within a Databricks environment. The ingestion process requires Supabase Storage credentials configured through Databricks Secrets. Workspace-specific pipeline configuration is not included in this repository.

Credentials and sensitive configuration values are intentionally excluded from version control.
---

**Author:** [tamakkj](https://github.com/tamakkj)
