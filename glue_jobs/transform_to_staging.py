import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, when, unix_timestamp, round, date_format, lit
)
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME, SOURCE_PATH, TARGET_PATH]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_PATH', 'TARGET_PATH'])

# ✅ Initialize Spark + Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("🚀 Starting Glue ETL Job:", args['JOB_NAME'])

# ✅ Step 1: Read data from S3 (raw CSV)
print("📥 Reading data from:", args['SOURCE_PATH'])
raw_dyf = glueContext.create_dynamic_frame.from_options(
    format_options={"withHeader": True},
    connection_type="s3",
    format="csv",
    connection_options={"paths": [args['SOURCE_PATH']]},
    transformation_ctx="raw_dyf"
)

# Convert DynamicFrame → DataFrame
df = raw_dyf.toDF()

# ✅ Step 2: Data Cleaning
df_clean = df.dropna(subset=["train_no", "scheduled_arrival", "actual_arrival"]) \
    .withColumn("scheduled_departure", col("scheduled_departure").cast("timestamp")) \
    .withColumn("actual_departure", col("actual_departure").cast("timestamp")) \
    .withColumn("scheduled_arrival", col("scheduled_arrival").cast("timestamp")) \
    .withColumn("actual_arrival", col("actual_arrival").cast("timestamp"))

# ✅ Step 3: Derive Delay Metrics
df_transformed = df_clean \
    .withColumn(
        "delay_departure_mins",
        round((unix_timestamp(col("actual_departure")) - unix_timestamp(col("scheduled_departure"))) / 60, 2)
    ) \
    .withColumn(
        "delay_arrival_mins",
        round((unix_timestamp(col("actual_arrival")) - unix_timestamp(col("scheduled_arrival"))) / 60, 2)
    )

# Handle early arrivals (negative delay → 0)
df_transformed = df_transformed.withColumn(
    "delay_departure_mins", when(col("delay_departure_mins") < 0, 0).otherwise(col("delay_departure_mins"))
).withColumn(
    "delay_arrival_mins", when(col("delay_arrival_mins") < 0, 0).otherwise(col("delay_arrival_mins"))
)

# ✅ Step 4: Derived Columns
df_transformed = df_transformed \
    .withColumn("day_of_week", date_format(col("date_of_journey"), "EEEE")) \
    .withColumn("status",
        when(col("delay_arrival_mins") == 0, lit("On Time"))
        .when((col("delay_arrival_mins") > 0) & (col("delay_arrival_mins") <= 30), lit("Slight Delay"))
        .when(col("delay_arrival_mins") > 30, lit("Heavily Delayed"))
        .otherwise(lit("Unknown"))
    )

# ✅ Step 5: Average Speed
df_transformed = df_transformed.withColumn(
    "travel_hours",
    round((unix_timestamp(col("actual_arrival")) - unix_timestamp(col("actual_departure"))) / 3600, 2)
).withColumn(
    "avg_speed_kmph",
    round(col("distance_km") / col("travel_hours"), 2)
)

# ✅ Step 6: Select and Write Processed Data Back to S3 (Parquet)
final_df = df_transformed.select(
    "train_no", "train_name", "source_station_code", "source_station_name",
    "destination_station_code", "destination_station_name",
    "scheduled_departure", "actual_departure",
    "scheduled_arrival", "actual_arrival",
    "delay_departure_mins", "delay_arrival_mins",
    "date_of_journey", "day_of_week", "train_type",
    "distance_km", "avg_speed_kmph", "status"
)

final_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_dyf")

print("💾 Writing transformed data to:", args['TARGET_PATH'])
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={"path": args['TARGET_PATH']},
    format="parquet",
    transformation_ctx="final_write"
)

print("✅ Transformation complete! Data successfully written to:", args['TARGET_PATH'])

job.commit()
