import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import avg, round, col
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME, STAGING_PATH, CURATED_PATH]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'STAGING_PATH', 'CURATED_PATH'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

print("🚀 Starting Curated Load Job:", args['JOB_NAME'])

# ✅ 1️⃣ Read data from STAGING (Parquet)
print("📥 Reading from staging layer:", args['STAGING_PATH'])
staging_dyf = glueContext.create_dynamic_frame.from_options(
    format="parquet",
    connection_type="s3",
    connection_options={"paths": [args['STAGING_PATH']]},
    transformation_ctx="staging_dyf"
)

df = staging_dyf.toDF()

# ✅ 2️⃣ Create Aggregated Views (Curated Data)
# Example 1: Average delay per train
avg_delay_per_train = df.groupBy("train_no", "train_name").agg(
    round(avg(col("delay_arrival_mins")), 2).alias("avg_arrival_delay"),
    round(avg(col("delay_departure_mins")), 2).alias("avg_departure_delay")
)

# Example 2: Average delay per route (source → destination)
avg_delay_per_route = df.groupBy("source_station_code", "destination_station_code").agg(
    round(avg(col("delay_arrival_mins")), 2).alias("avg_delay_route")
)

# ✅ 3️⃣ Write aggregated outputs to Curated S3
avg_delay_per_train_dyf = DynamicFrame.fromDF(avg_delay_per_train, glueContext, "avg_delay_per_train_dyf")
avg_delay_per_route_dyf = DynamicFrame.fromDF(avg_delay_per_route, glueContext, "avg_delay_per_route_dyf")

print("💾 Writing curated data to:", args['CURATED_PATH'])
glueContext.write_dynamic_frame.from_options(
    frame=avg_delay_per_train_dyf,
    connection_type="s3",
    connection_options={"path": args['CURATED_PATH'] + "avg_delay_per_train/"},
    format="parquet"
)

glueContext.write_dynamic_frame.from_options(
    frame=avg_delay_per_route_dyf,
    connection_type="s3",
    connection_options={"path": args['CURATED_PATH'] + "avg_delay_per_route/"},
    format="parquet"
)

print("✅ Curated data written successfully!")

job.commit()
