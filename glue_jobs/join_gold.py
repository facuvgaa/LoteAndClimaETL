
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "BUCKET", "CAMPANA",
])

sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)

bucket = args["BUCKET"]
campana = args["CAMPANA"]

rinde_path = f"s3://{bucket}/silver/rinde_lotes/"
clima_path = f"s3://{bucket}/silver/clima_diario/"
target_path = f"s3://{bucket}/gold/rinde_clima/"

rinde = spark.read.parquet(rinde_path).filter(F.col("campaña") == campana)
clima = spark.read.parquet(clima_path)

clima_agg = (
    clima
    .groupBy("lote")
    .agg(
        F.round(F.avg("temp_max"), 1).alias("avg_temp_max"),
        F.round(F.avg("temp_min"), 1).alias("avg_temp_min"),
        F.round(F.sum("precipitacion_mm"), 1).alias("total_precip_mm"),
        F.round(F.avg("humedad_pct"), 1).alias("avg_humedad_pct"),
        F.count("*").alias("dias_con_datos_clima"),
    )
)

gold = (
    rinde
    .join(clima_agg, on="lote", how="left")
    .withColumn("rinde_por_mm_precip",
                F.when(F.col("total_precip_mm") > 0,
                       F.round(F.col("rinde_kg_ha") / F.col("total_precip_mm"), 2))
                .otherwise(None))
)

(
    gold.write
    .mode("overwrite")
    .option("partitionOverwriteMode", "dynamic")
    .partitionBy("campaña", "lote")
    .parquet(target_path)
)

job.commit()
