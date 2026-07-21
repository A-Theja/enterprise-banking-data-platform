# Enterprise Banking Data Platform

A portfolio-grade data engineering project that demonstrates an end-to-end lakehouse pipeline using Azure Data Factory, Azure Databricks, PySpark, Delta Lake, Snowflake/dbt, and Power BI.

## Business problem

A fictional bank receives customer, account, transaction, and exchange-rate data from multiple systems. The goal is to build a reliable analytics platform that:

- Ingests batch and API data
- Preserves raw history
- Cleans and standardizes records
- Supports incremental processing
- Produces reporting-ready fact and dimension tables
- Validates data quality
- Provides monitoring and deployment documentation

## Planned architecture

Sources -> Azure Data Factory -> ADLS Bronze -> Databricks/PySpark -> Delta Silver/Gold -> Snowflake/dbt -> Power BI

## Initial source datasets

- Customers: CSV
- Accounts: CSV
- Transactions: CSV/JSON
- Exchange rates: REST API

## Core outputs

- `dim_customer`
- `dim_account`
- `dim_date`
- `fact_transaction`
- Daily transaction KPI dataset

## Repository structure

- `adf/`: ADF pipeline definitions and documentation
- `databricks/`: Bronze, Silver, and Gold notebooks
- `dbt/`: dbt models and tests
- `sql/`: DDL and validation queries
- `powerbi/`: dashboard documentation and screenshots
- `docs/`: architecture, requirements, and runbooks
- `data/`: small synthetic datasets for development
- `tests/`: data quality and unit-test examples
- `config/`: environment configuration templates

## Delivery plan

1. Define requirements and data contracts
2. Generate and validate sample source data
3. Build Bronze ingestion
4. Build Silver transformations
5. Build Gold dimensional model
6. Add incremental loading and MERGE logic
7. Add data quality checks
8. Add dbt/Snowflake layer
9. Build Power BI dashboard
10. Add CI/CD, monitoring, and interview documentation



