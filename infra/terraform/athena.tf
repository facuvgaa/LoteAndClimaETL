resource "aws_athena_workgroup" "main" {
  name = "${var.project_name}-${var.environment}"

  configuration {
    enforce_workgroup_configuration = true

    result_configuration {
      output_location = "s3://${var.bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }

    bytes_scanned_cutoff_per_query = 1073741824 # 1 GB limit
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_athena_named_query" "create_gold_table" {
  name      = "create_rinde_clima_gold"
  workgroup = aws_athena_workgroup.main.name
  database  = aws_glue_catalog_database.main.name

  query = <<-SQL
    CREATE EXTERNAL TABLE IF NOT EXISTS rinde_clima (
      lote              STRING,
      cultivo           STRING,
      rinde_kg_ha       DOUBLE,
      superficie_ha     DOUBLE,
      fecha_cosecha     DATE,
      avg_temp_max      DOUBLE,
      avg_temp_min      DOUBLE,
      total_precip_mm   DOUBLE,
      avg_humedad_pct   DOUBLE,
      dias_con_datos_clima INT,
      rinde_por_mm_precip  DOUBLE
    )
    PARTITIONED BY (campaña STRING, lote_part STRING)
    STORED AS PARQUET
    LOCATION 's3://${var.bucket_name}/gold/rinde_clima/'
    TBLPROPERTIES ('classification'='parquet')
  SQL
}
