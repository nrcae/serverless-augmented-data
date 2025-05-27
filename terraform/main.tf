# main.tf - Core AWS resources for the Serverless Data Insight Tool

provider "aws" {
  region = var.aws_region
}

# --- S3 Buckets ---
resource "aws_s3_bucket" "input_bucket" {
  bucket = var.input_bucket_name
  # versioning {
  #   enabled = true
  # }
  # server_side_encryption_configuration {
  #   rule {
  #     apply_server_side_encryption_by_default {
  #       sse_algorithm = "AES256"
  #     }
  #   }
  # }
  tags = {
    Name        = "AI Dataset Input Bucket"
    Environment = var.environment
    Project     = "AIDatasetInsightTool"
  }
}

resource "aws_s3_bucket" "output_bucket" {
  bucket = var.output_bucket_name
  # versioning {
  #   enabled = true
  # }
  # server_side_encryption_configuration {
  #   rule {
  #     apply_server_side_encryption_by_default {
  #       sse_algorithm = "AES256"
  #     }
  #   }
  # }
  tags = {
    Name        = "AI Dataset Output Bucket"
    Environment = var.environment
    Project     = "AIDatasetInsightTool"
  }
}

# --- IAM Role and Policy for Lambda ---
data "aws_iam_policy_document" "lambda_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_execution_role" {
  name               = "${var.lambda_function_name}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role_policy.json
  tags = {
    Name        = "${var.lambda_function_name}-ExecutionRole"
    Environment = var.environment
    Project     = "AIDatasetInsightTool"
  }
}

data "aws_iam_policy_document" "lambda_permissions_policy" {
  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["arn:aws:logs:*:*:*"] # Restrict if possible
  }

  statement {
    actions = [
      "s3:GetObject"
    ]
    resources = ["${aws_s3_bucket.input_bucket.arn}/*"] # Read from input bucket
  }

  statement {
    actions = [
      "s3:PutObject"
    ]
    resources = ["${aws_s3_bucket.output_bucket.arn}/*"] # Write to output bucket
  }
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.lambda_function_name}-policy"
  description = "IAM policy for Lambda to access S3 and CloudWatch Logs"
  policy      = data.aws_iam_policy_document.lambda_permissions_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attach" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# --- Lambda Function ---
# Assumes lambda_deployment_package.zip is in the same directory or specified path
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = var.lambda_source_code_path
  output_path = "lambda_deployment_package.zip"
}

resource "aws_lambda_function" "dataset_insight_lambda" {
  function_name    = var.lambda_function_name
  handler          = var.lambda_handler_name
  runtime          = var.lambda_runtime
  role             = aws_iam_role.lambda_execution_role.arn
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = var.lambda_timeout_seconds
  memory_size      = var.lambda_memory_mb

  environment {
    variables = {
      OPENAI_API_KEY                   = var.openai_api_key
      OUTPUT_BUCKET_NAME               = aws_s3_bucket.output_bucket.bucket
      OUTPUT_FORMAT                    = var.output_format
      TEXT_COLUMN_NAME                 = var.text_column_name
      PROMPT_STRATEGY                  = var.prompt_strategy
      MAX_CHARS_FOR_SUMMARY_PROMPT     = var.max_chars_for_summary_prompt
      MAX_RECORDS_FOR_SUMMARY          = var.max_records_for_summary
      DYNAMODB_TABLE_NAME              = aws_dynamodb_table.insights_table.name
    }
  }

  tags = {
    Name        = var.lambda_function_name
    Environment = var.environment
    Project     = "AIDatasetInsightTool"
  }

  depends_on = [
    aws_iam_role_policy_attachment.lambda_policy_attach,
    aws_s3_bucket.input_bucket,
    aws_s3_bucket.output_bucket
  ]
}

# --- S3 Bucket Notification for Lambda Trigger ---
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.input_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.dataset_insight_lambda.arn
    events              = ["s3:ObjectCreated:*"] # Trigger on all object creation events
    # filter_prefix       = "uploads/"
    filter_suffix       = ".csv"
  }

  depends_on = [aws_lambda_function.dataset_insight_lambda]
}

# --- Lambda Permission for S3 to Invoke ---
resource "aws_lambda_permission" "allow_s3_to_invoke_lambda" {
  statement_id  = "AllowS3ToInvokeLambda"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.dataset_insight_lambda.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.input_bucket.arn
  source_account = data.aws_caller_identity.current.account_id
}

# --- DynamoDB Table ---
resource "aws_dynamodb_table" "insights_table" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST" # On-demand capacity
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  tags = {
    Environment = "dev"
    Project     = "AIDatasetInsightTool"
  }
}