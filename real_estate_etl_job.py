import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

print("=" * 70)
print("Real Estate ETL Job - Complete Version")
print("=" * 70)

# Configuration
RAW_BUCKET = "kaggle-realestate-pipeline-raw-group4"
PROCESSED_BUCKET = "kaggle-realestate-pipeline-processed-group4"

ZILLOW_PATH = f"s3://{RAW_BUCKET}/Zillow/realtor-data.csv"
CENSUS_PATH = f"s3://{RAW_BUCKET}/census/census_multistate_data.json"
OUTPUT_PATH = f"s3://{PROCESSED_BUCKET}/enriched_real_estate_data/"

# STEP 1: Read Zillow Data
print("\n" + "=" * 70)
print("STEP 1: Reading Zillow Data")
print("=" * 70)

zillow_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [ZILLOW_PATH]},
    format="csv",
    format_options={"withHeader": True, "separator": ","}
).toDF()

total_rows = zillow_df.count()
print(f"Read {total_rows:,} properties from Zillow")

# STEP 2: Filter to Target States
print("\n" + "=" * 70)
print("STEP 2: Filtering to MA, CA, NY")
print("=" * 70)

target_states = ['Massachusetts', 'California', 'New York']
zillow_filtered = zillow_df.filter(F.col("state").isin(target_states))

filtered_count = zillow_filtered.count()
print(f"Filtered to {filtered_count:,} properties")

state_counts = zillow_filtered.groupBy("state").count().orderBy("state").collect()
for row in state_counts:
    print(f"   {row['state']}: {row['count']:,}")

# STEP 3: Clean Data
print("\n" + "=" * 70)
print("STEP 3: Cleaning Data")
print("=" * 70)

zillow_clean = zillow_filtered.filter(
    F.col("price").isNotNull() &
    F.col("zip_code").isNotNull() &
    F.col("house_size").isNotNull() &
    F.col("bed").isNotNull() &
    F.col("bath").isNotNull()
)

clean_count = zillow_clean.count()
print(f"After cleaning: {clean_count:,} properties")

# STEP 4: Sample 100 per State
print("\n" + "=" * 70)
print("STEP 4: Sampling 100 Properties per State")
print("=" * 70)

window = Window.partitionBy("state").orderBy(F.rand())
zillow_sampled = zillow_clean.withColumn("rn", F.row_number().over(window))
zillow_sampled = zillow_sampled.filter(F.col("rn") <= 100).drop("rn")

sampled_count = zillow_sampled.count()
print(f"Sampled: {sampled_count} properties")

sample_dist = zillow_sampled.groupBy("state").count().orderBy("state").collect()
for row in sample_dist:
    print(f"   {row['state']}: {row['count']}")

# STEP 5: Read Census Data
print("\n" + "=" * 70)
print("STEP 5: Reading Census Data")
print("=" * 70)

# Use multiLine option 
census_df = spark.read.option("multiLine", "true").json(CENSUS_PATH)

census_count = census_df.count()
print(f"Read {census_count} ZIP codes from Census")
print(f"Census columns: {census_df.columns}")

# STEP 6: Join Zillow + Census
print("\n" + "=" * 70)
print("STEP 6: Joining Housing + Demographics")
print("=" * 70)

# Join on zip_code
enriched_df = zillow_sampled.join(
    F.broadcast(census_df),
    on="zip_code",
    how="left"
)

joined_count = enriched_df.count()
print(f"Joined: {joined_count} properties")

with_census = enriched_df.filter(F.col("median_income").isNotNull()).count()
print(f"Properties with census match: {with_census}")

# STEP 7: Adding Calculated Fields
print("\n" + "=" * 70)
print("STEP 7: Adding Calculated Fields")
print("=" * 70)

# Cast and calculate price per sqft
enriched_df = enriched_df.withColumn("price_num", F.col("price").cast("double"))
enriched_df = enriched_df.withColumn("house_size_num", F.col("house_size").cast("double"))
enriched_df = enriched_df.withColumn(
    "price_per_sqft",
    F.round(F.col("price_num") / F.col("house_size_num"), 2)
)

print("Added price_per_sqft calculation")

# STEP 8: Select Final Columns
print("\n" + "=" * 70)
print("STEP 8: Selecting Final Columns")
print("=" * 70)

final_df = enriched_df.select(
    "brokered_by",
    "status",
    "price",
    "bed",
    "bath",
    "acre_lot",
    "street",
    "city",
    zillow_sampled["state"],
    "zip_code",
    "house_size",
    "prev_sold_date",
    F.col("median_income").alias("census_median_income"),
    F.col("population").alias("census_population"),
    F.col("college_educated_pct").alias("census_college_pct"),
    F.col("unemployment_rate").alias("census_unemployment_rate"),
    F.col("median_age").alias("census_median_age"),
    "price_per_sqft"
)

print(f"Final columns: {final_df.columns}")

# STEP 9: Write to Processed Bucket
print("\n" + "=" * 70)
print("STEP 9: Writing to Processed Bucket")
print("=" * 70)

final_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_PATH)

print(f"Data written to: {OUTPUT_PATH}")

# FINAL SUMMARY
print("\n" + "=" * 70)
print("ETL JOB COMPLETE!")
print("=" * 70)
print(f"\nSummary:")
print(f"   Total input rows: {total_rows:,}")
print(f"   After state filter: {filtered_count:,}")
print(f"   After cleaning: {clean_count:,}")
print(f"   Final sampled: {sampled_count}")
print(f"   Census ZIP codes: {census_count}")
print(f"   Properties with census data: {with_census}")
print(f"   Output location: {OUTPUT_PATH}")
print("\nReal estate data successfully processed and enriched!")
print("=" * 70)

job.commit()