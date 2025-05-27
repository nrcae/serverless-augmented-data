# --- outputs.tf ---
output "lambda_function_arn" {
  description = "ARN of the created Lambda function."
  value       = aws_lambda_function.dataset_insight_lambda.arn
}

output "lambda_function_name_out" {
  description = "Name of the created Lambda function."
  value       = aws_lambda_function.dataset_insight_lambda.function_name
}

output "lambda_iam_role_arn" {
  description = "ARN of the IAM role for the Lambda function."
  value       = aws_iam_role.lambda_execution_role.arn
}

output "input_s3_bucket_name" {
  description = "Name of the S3 input bucket."
  value       = aws_s3_bucket.input_bucket.bucket
}

output "output_s3_bucket_name" {
  description = "Name of the S3 output bucket."
  value       = aws_s3_bucket.output_bucket.bucket
}

output "dynamodb_table_name" {
  description = "The name of the DynamoDB table."
  value       = aws_dynamodb_table.insights_table.name
}
