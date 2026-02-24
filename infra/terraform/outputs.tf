output "bucket_name" {
  description = "S3 data lake bucket name"
  value       = aws_s3_bucket.data_lake.id
}

output "bucket_arn" {
  description = "S3 data lake bucket ARN"
  value       = aws_s3_bucket.data_lake.arn
}

output "step_function_arn" {
  description = "Step Function state machine ARN"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "glue_job_names" {
  description = "Names of the Glue Jobs"
  value = {
    ingest = aws_glue_job.ingest_bronze.name
    clean  = aws_glue_job.clean_silver.name
    join   = aws_glue_job.join_gold.name
  }
}

output "glue_catalog_database" {
  description = "Glue Catalog database name"
  value       = aws_glue_catalog_database.main.name
}

output "athena_workgroup" {
  description = "Athena workgroup name"
  value       = aws_athena_workgroup.main.name
}
