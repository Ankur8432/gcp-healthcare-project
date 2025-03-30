from pyspark.sql import SparkSession
from pyspark.sql.functions import when, input_file_name

# Create Spark session
spark = SparkSession.builder \
    .appName("Healthcare Claims Ingestion") \
    .getOrCreate()

BUCKET_NAME = "health_care_buckets"
CLAIMS_BUCKET_PATH = f"gs://{BUCKET_NAME}/landing/claims/*.csv"
BQ_TABLE = "avd-group-gcp.bronze_dataset.claims"
TEMP_GCS_BUCKET = f"{BUCKET_NAME}/temp/"

# Read from claims sources
claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

# Add datasource column based on file name
claims_df = claims_df.withColumn(
    "datasource",
    when(input_file_name().contains("hospital2"), "hosb")
    .when(input_file_name().contains("hospital1"), "hosa")
    .otherwise("None")
)

# Write DataFrame to BigQuery
claims_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("temporaryGcsBucket", TEMP_GCS_BUCKET) \
    .mode("overwrite") \
    .save()
