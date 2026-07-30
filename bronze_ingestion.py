# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion
# MAGIC
# MAGIC Reads raw customer, account, and transaction CSV files and writes them
# MAGIC to Bronze Delta tables with ingestion audit columns.
# MAGIC
# MAGIC Bronze rules:
# MAGIC - Preserve source values as received
# MAGIC - Use explicit schemas
# MAGIC - Add audit metadata
# MAGIC - Append new batches
# MAGIC - Do not apply business transformations here

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
)

# COMMAND ----------

# Parameters can be passed from Azure Data Factory or a Databricks workflow.
dbutils.widgets.text("source_base_path", "")
dbutils.widgets.text("bronze_base_path", "")
dbutils.widgets.text("pipeline_run_id", "manual-run")

source_base_path = dbutils.widgets.get("source_base_path").rstrip("/")
bronze_base_path = dbutils.widgets.get("bronze_base_path").rstrip("/")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

if not source_base_path:
    raise ValueError("source_base_path is required.")

if not bronze_base_path:
    raise ValueError("bronze_base_path is required.")

# COMMAND ----------

customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

account_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("account_type", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("opened_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("transaction_ts", StringType(), True),
    StructField("amount", DecimalType(18, 2), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("status", StringType(), True),
    StructField("updated_at", StringType(), True),
])

# COMMAND ----------

def ingest_csv_to_bronze(
    dataset_name: str,
    source_path: str,
    target_path: str,
    schema: StructType,
) -> None:
    """Read one raw CSV dataset and append it to a Bronze Delta location."""

    source_df = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(source_path)
    )

    bronze_df = (
        source_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingestion_date", F.current_date())
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("_dataset_name", F.lit(dataset_name))
    )

    (
        bronze_df.write
        .format("delta")
        .mode("append")
        .partitionBy("_ingestion_date")
        .save(target_path)
    )

    print(
        f"Bronze ingestion completed for {dataset_name}. "
        f"Rows written: {bronze_df.count()}; target: {target_path}"
    )

# COMMAND ----------

datasets = [
    {
        "name": "customers",
        "source": f"{source_base_path}/customers/*.csv",
        "target": f"{bronze_base_path}/customers",
        "schema": customer_schema,
    },
    {
        "name": "accounts",
        "source": f"{source_base_path}/accounts/*.csv",
        "target": f"{bronze_base_path}/accounts",
        "schema": account_schema,
    },
    {
        "name": "transactions",
        "source": f"{source_base_path}/transactions/*.csv",
        "target": f"{bronze_base_path}/transactions",
        "schema": transaction_schema,
    },
]

for dataset in datasets:
    ingest_csv_to_bronze(
        dataset_name=dataset["name"],
        source_path=dataset["source"],
        target_path=dataset["target"],
        schema=dataset["schema"],
    )

# COMMAND ----------

# Optional validation
for dataset in ["customers", "accounts", "transactions"]:
    df = spark.read.format("delta").load(f"{bronze_base_path}/{dataset}")
    print(f"{dataset}: {df.count()} Bronze rows")
