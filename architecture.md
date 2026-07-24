# Architecture Notes

## Logical flow

1. Source systems provide customer, account, transaction, and exchange-rate data.
2. Azure Data Factory orchestrates ingestion.
3. Raw data lands in ADLS Gen2 Bronze storage.
4. Databricks and PySpark validate, clean, and standardize data into Silver Delta tables.
5. Gold models create dimensions, facts, and daily aggregates.
6. Snowflake and dbt provide an optional serving and transformation layer.
7. Power BI consumes curated Gold or Snowflake datasets.

## Design decisions
- Bronze is append-only to preserve source history.
- Silver contains validated and standardized records.
- Gold contains business-ready dimensional models.
- Transaction ID is the primary deduplication key.
- `updated_at` is used as the incremental watermark.
- Audit fields include ingestion timestamp, source file, and pipeline run ID.
