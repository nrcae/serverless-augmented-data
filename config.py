# config.py
# Environment variables

import os

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
OUTPUT_BUCKET_NAME = os.environ.get('OUTPUT_BUCKET_NAME')
OUTPUT_FORMAT = os.environ.get('OUTPUT_FORMAT', 'json').lower()
TEXT_COLUMN_NAME = os.environ.get('TEXT_COLUMN_NAME', 'text_column')
# Prompting strategy
# 'per_record': Generates an insight for each record (default).
# 'summarize_all': Concatenates text from all records and generates a single insight.
PROMPT_STRATEGY = os.environ.get('PROMPT_STRATEGY', 'per_record').lower()
MAX_CHARS_FOR_SUMMARY_PROMPT = int(os.environ.get('MAX_CHARS_FOR_SUMMARY_PROMPT', 15000)) # Max chars for summarize_all prompt (OpenAI token limits)
MAX_RECORDS_FOR_SUMMARY = int(os.environ.get('MAX_RECORDS_FOR_SUMMARY', 100)) # Max records to include in 'summarize_all' to avoid excessive data
# DynamoDB specific environment variable
DYNAMODB_TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME')
