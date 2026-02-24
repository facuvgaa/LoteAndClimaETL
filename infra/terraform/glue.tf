resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}_${var.environment}"
}

resource "aws_glue_job" "ingest_bronze" {
  name     = "${var.project_name}-${var.environment}-ingest-bronze"
  role_arn = aws_iam_role.glue_ingest.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/ingest_bronze.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  execution_property {
    max_concurrent_runs = 2
  }

  default_arguments = {
    "--job-language"               = "python"
    "--enable-auto-scaling"        = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${var.bucket_name}/tmp/ingest/"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "bronze"
  }
}

resource "aws_glue_job" "clean_silver" {
  name     = "${var.project_name}-${var.environment}-clean-silver"
  role_arn = aws_iam_role.glue_clean.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/clean_silver.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  execution_property {
    max_concurrent_runs = 2
  }

  default_arguments = {
    "--job-language"               = "python"
    "--enable-auto-scaling"        = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${var.bucket_name}/tmp/clean/"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "silver"
  }
}

resource "aws_glue_job" "join_gold" {
  name     = "${var.project_name}-${var.environment}-join-gold"
  role_arn = aws_iam_role.glue_join.arn

  command {
    name            = "glueetl"
    script_location = "s3://${var.bucket_name}/scripts/join_gold.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2

  execution_property {
    max_concurrent_runs = 1
  }

  default_arguments = {
    "--job-language"               = "python"
    "--enable-auto-scaling"        = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--TempDir"                    = "s3://${var.bucket_name}/tmp/join/"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Layer       = "gold"
  }
}
