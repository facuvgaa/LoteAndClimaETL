import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DoubleType

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

source_path = f"s3://{bucket}/bronze/{dataset}/"
target_path = f"s3://{bucket}/silver/{dataset}/"

df = spark.read.parquet(source_path)

if "campaña" in df.columns:
    df = df.filter(F.col("campaña") == campana)

# --- Rinde-specific cleaning ---
if dataset == "rinde_lotes":
    df = (
        df
        .withColumn("rinde_kg_ha", F.col("rinde_kg_ha").cast(DoubleType()))
        .withColumn("superficie_ha", F.col("superficie_ha").cast(DoubleType()))
        .withColumn("fecha_cosecha", F.to_date(F.col("fecha_cosecha"), "yyyy-MM-dd"))
        .filter(F.col("fecha_cosecha").isNotNull())
        .filter(F.col("rinde_kg_ha").between(0, 25000) | F.col("rinde_kg_ha").isNull())
        .dropDuplicates(["campaña", "lote", "fecha_cosecha"])
    )
    partition_keys = ["campaña", "lote"]

# --- Clima-specific cleaning ---
elif dataset == "clima_diario":
    df = (
        df
        .withColumn("fecha", F.to_date(F.col("fecha"), "yyyy-MM-dd"))
        .withColumn("temp_max", F.col("temp_max").cast(DoubleType()))
        .withColumn("temp_min", F.col("temp_min").cast(DoubleType()))
        .withColumn("precipitacion_mm", F.col("precipitacion_mm").cast(DoubleType()))
        .withColumn("humedad_pct", F.col("humedad_pct").cast(DoubleType()))
        .filter(F.col("fecha").isNotNull())
        .filter(F.col("temp_max").between(-20, 55) | F.col("temp_max").isNull())
        .filter(F.col("temp_min").between(-30, 45) | F.col("temp_min").isNull())
        .filter(F.col("precipitacion_mm").between(0, 500) | F.col("precipitacion_mm").isNull())
        .filter(F.col("humedad_pct").between(0, 100) | F.col("humedad_pct").isNull())
        .dropDuplicates(["fecha", "lote"])
    )
    partition_keys = ["lote"]

else:
    raise ValueError(f"Unknown dataset: {dataset}")

(
    df.write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy(*partition_keys)
    .parquet(target_path)
)

job.commit()
