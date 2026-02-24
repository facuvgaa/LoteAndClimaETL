resource "aws_sfn_state_machine" "pipeline" {
  name     = "${var.project_name}-${var.environment}-pipeline"
  role_arn = aws_iam_role.step_function.arn

  definition = templatefile("${path.module}/../step_function.json", {})

  type = "STANDARD"

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_function.arn}:*"
    include_execution_data = true
    level                  = "ERROR"
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "step_function" {
  name              = "/aws/states/${var.project_name}-${var.environment}-pipeline"
  retention_in_days = 30

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}
