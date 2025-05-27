# --- variables.tf ---
variable "aws_region" {
  description = "AWS region to deploy resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (e.g., dev, staging, prod)."
  type                = string
  default     = "dev"
}

variable "input_bucket_name" {
  description = "Name for the S3 input bucket (must be globally unique)."
  type        = string
  default     = "my-ai-dataset-input-bucket-unique"
}

variable "output_bucket_name" {
  description = "Name for the S3 output bucket (must be globally unique)."
  type        = string
  default     = "my-ai-dataset-output-bucket-unique"
}

variable "dynamodb_table_name" {
  description = "Name for the DynamoDB table to store insights."
  type        = string
  default     = "AIDatasetInsights"
}

variable "lambda_function_name" {
  description = "Name for the Lambda function."
  type        = string
  default     = "AIDatasetInsightFunction"
}

variable "lambda_source_code_path" {
  description = "Path to the directory containing Lambda source code (e.g. './src' or './')."
  type        = string
  default     = "./"
}

variable "lambda_handler_name" {
  description = "Lambda handler (filename.function_name)."
  type        = string
  default     = "lambda_function.lambda_handler"
}

variable "lambda_runtime" {
  description = "Lambda runtime environment."
  type        = string
  default     = "python3.9"
}

variable "lambda_timeout_seconds" {
  description = "Lambda execution timeout in seconds."
  type        = number
  default     = 300 # 5 minutes
}

variable "lambda_memory_mb" {
  description = "Lambda memory allocation in MB."
  type        = number
  default     = 512
}

variable "openai_api_key" {
  description = "OpenAI API Key. For production, use AWS Secrets Manager."
  type        = string
  sensitive   = true
  # default     = "openai-api-key-here"
}

variable "output_format" {
  description = "Output format for augmented data (json, csv, parquet)."
  type        = string
  default     = "json"
}

variable "text_column_name" {
  description = "Default column name for text analysis."
  type        = string
  default     = "text_column"
}

variable "prompt_strategy" {
  description = "Prompting strategy for insight generation."
  type        = string
  default     = "per_record"
  validation {
    condition     = contains(["per_record", "summarize_all"], var.prompt_strategy)
    error_message = "Valid prompt strategies are: per_record, summarize_all."
  }
}

variable "max_chars_for_summary_prompt" {
  description = "Max characters for 'summarize_all' prompt."
  type        = number
  default     = 15000
}

variable "max_records_for_summary" {
  description = "Max records for 'summarize_all' prompt."
  type        = number
  default     = 100
}
variable "alarm_notification_email" {
  description = "Email address for alarm notifications. Leave blank to disable email subscription."
  type        = string
  default     = "" # Example: "email@example.com"
}

variable "lambda_duration_alarm_threshold_ms" {
  description = "Threshold in milliseconds for Lambda duration alarm. Set to 0 to disable this alarm."
  type        = number
  default     = 0 # e.g., 240000 for 4 minutes (80% of 5 min timeout)
}