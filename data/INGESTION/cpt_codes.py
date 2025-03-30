# Databricks notebook source
from pyspark.sql import SparkSession, functions as f

# Create Spark session
spark = SparkSession.builder \
    .appName("CPT Codes Ingestion") \
    .getOrCreate()

BUCKET_NAME = "health_care_buckets"
CPT_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv"
BQ_TABLE = "avd-group-gcp.bronze_dataset.cpt_codes"
TEMP_GCS_BUCKET = f"health_care_buckets/temp/"

# Read the CSV file
cptcodes_df = spark.read.csv(f"gs://{BUCKET_NAME}/landing/cptcodes/*.csv", header=True)

# Replace whitespaces in column names with underscores and convert to lowercase
for col in cptcodes_df.columns:
    new_col = col.replace(" ", "_").lower()
    cptcodes_df = cptcodes_df.withColumnRenamed(col, new_col)

# Write DataFrame to BigQuery
cptcodes_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()
