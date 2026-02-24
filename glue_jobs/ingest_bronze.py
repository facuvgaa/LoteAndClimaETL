import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "BUCKET", "DATASET", "CAMPANA",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

bucket = args["BUCKET"]
dataset = args["DATASET"]
campana = args["CAMPANA"]

source_path = f"s3://{bucket}/raw/{dataset}.csv"
target_path = f"s3://{bucket}/bronze/{dataset}/"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)

df_filtered = df.filter(df["campaña"] == campana) if "campaña" in df.columns else df

(
    df_filtered.write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("campaña" if "campaña" in df.columns else [])
    .parquet(target_path)
)

job.commit()
