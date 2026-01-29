# Securities Pricing Data Pipeline

🚀 **End-to-End Data Engineering Pipeline for Securities Pricing Analytics**

This project implements a complete, production-style data pipeline for ingesting, transforming, storing, and visualizing securities pricing data. It demonstrates real-world data engineering practices using **Docker, Apache Airflow, AWS S3, Snowflake, Python, SQL, and Power BI**.

> ⚠️ Note: *Polygon.io is now known as **Massive.com***. All references to Polygon in this project refer to the Massive (formerly Polygon) market data API.

---

## 📌 Project Overview

The goal of this project is to design and build a **scalable, automated, and analytics-ready data pipeline** that:

* Collects daily securities pricing data from Massive (formerly Polygon.io)
* Orchestrates ingestion and processing with Apache Airflow
* Stores raw and transformed data in Snowflake
* Applies dimensional modeling (facts & dimensions)
* Enforces data quality checks
* Sends alerts via Slack
* Exposes analytics-ready datasets for reporting in Power BI

This repository includes:

* All **Python ingestion scripts**
* All **SQL transformation scripts**
* Airflow **DAGs**
* Snowflake **schema design**
* Power BI **final analytics output**
* Dockerized infrastructure

---

## 🏗️ Architecture

![architecture](project_architecture.png)

**Data Flow:**

1. **Massive.com (formerly Polygon.io)**

   * Market data API source for securities pricing data

2. **Docker + Apache Airflow**

   * Orchestrates ingestion, transformation, alerting, and data quality checks
   * Runs as a containerized workflow platform

3. **AWS S3**

   * Stores raw extracted files from Massive.com
   * Acts as the staging area for Snowflake ingestion

4. **Snowflake**

   * Data warehouse with layered architecture:

     * `RAW` → Unprocessed data
     * `CORE` → Cleaned and standardized tables
     * `DIM` → Dimension tables
     * `FACTS` → Fact tables
     * `SA` → Serving / analytics-ready tables

5. **Slack**

   * Receives pipeline alerts, failures, and data validation notifications

6. **Power BI**

   * Connects to Snowflake for analytics dashboards and reporting

---

## 🧱 Data Warehouse Design

This project follows a **modern layered warehouse architecture**:

| Layer | Purpose                                           |
| ----- | ------------------------------------------------- |
| RAW   | Stores ingested data as-is from Massive           |
| CORE  | Standardized, typed, cleaned data                 |
| DIM   | Dimension tables (security, date, exchange, etc.) |
| FACTS | Fact tables containing pricing metrics            |
| SA    | Optimized serving layer for BI tools              |

---

## ⚙️ Technologies Used

| Category         | Tools                             |
| ---------------- | --------------------------------- |
| Orchestration    | Apache Airflow                    |
| Containerization | Docker                            |
| Data Warehouse   | Snowflake                         |
| Cloud Storage    | AWS S3                            |
| Programming      | Python                            |
| Transformations  | SQL                               |
| Monitoring       | Slack Webhooks                    |
| BI & Reporting   | Power BI                          |
| Source Data      | Massive.com (formerly Polygon.io) |

---

## 📂 Repository Structure

```text
.
├── architecture/                     # Architecture diagram(s)
│   └── project_architecture.png
│
├── dags/                             # Airflow DAG definitions
│   └── <your_dag_files>.py
│
├── lib/                              # Reusable Python modules (helpers/utilities)
│   ├── eod_data_downloader.py
│   └── slack_utils.py
│
├── sql/                              # Snowflake SQL scripts (ETL/ELT steps)
│   ├── 1_copy_to_raw.sql
│   ├── 2_check_loaded.sql
│   ├── 3_premerge_metrics.sql
│   ├── 4_merge_core.sql
│   ├── 5_merge_dim_security.sql
│   ├── 6_dm_dim_date.sql
│   ├── 7_merge_fact_daily_price.sql
│   └── 8_postmerge_metrics.sql
│
├── scripts/                          # Standalone scripts / utilities (optional)
│   └── get_securities_data.py
│
├── tests/                            # Connectivity / validation tests
│   ├── test_aws_conn.py
│   ├── test_slack_conn.py
│   └── test_snowflake_conn.py
│
├── dashboard/                        # Power BI & report exports
│   ├── securities_market_insights.pbix
│   ├── securities_market_report1.jpg
│   └── securities_market_report2.jpg
│
├── docker-compose.yaml               # Local stack for Airflow/Docker services
├── .env.example                      # Template for environment variables
├── airflow.cfg.example               # Template Airflow config (no secrets)
├── .gitignore
└── README.md
```

---

## 🔄 Pipeline Workflow

1. Airflow triggers ingestion DAG
2. Python scripts fetch data from Massive.com API
3. Data is written to AWS S3
4. Snowflake stages load files via `COPY INTO`
5. SQL transforms populate:

   * RAW → CORE → DIM/FACT → SA layers
6. Data quality checks validate:

   * Record counts
   * Null values
   * Price ranges
7. Alerts sent to Slack on failure or anomalies
8. Power BI consumes final SA tables for reporting

---

## 📊 Power BI Reporting

Power BI connects directly to Snowflake’s serving layer and provides:

* Price trend analysis
* Security performance tracking
* Historical market behavior
* Analytical insights from structured fact tables

Dashboard is generated from the same pipeline output, proving full **end-to-end data integrity**.

---

## 🔐 Configuration & Security

Sensitive files are **not included** in this repository:

* `.env`
* `airflow.cfg`
* API keys
* Snowflake credentials

---

## ✨ Author

**Oluwaseyi Akinsanya**
Data Engineer / Data Scientist

LinkedIn: [https://www.linkedin.com/in/seyi-a-852314184](https://www.linkedin.com/in/seyi-a-852314184)

