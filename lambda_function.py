# lambda_function.py
# Main AWS Lambda handler for the AI Dataset Insight Tool.

import json
import os
import boto3
import logging
import csv
import io
from utils.openai_utils import get_openai_insights
from utils.data_utils import process_data

# For Parquet output, if supported
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client
s3_client = boto3.client('s3')

# Environment variables
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

def get_text_from_record(record, index):
    """Helper to extract text from a record based on configured column names."""
    text_to_analyze = None
    if TEXT_COLUMN_NAME in record and isinstance(record[TEXT_COLUMN_NAME], str):
        text_to_analyze = record[TEXT_COLUMN_NAME]
    elif 'text_content' in record and isinstance(record['text_content'], str):
        text_to_analyze = record['text_content']
    elif 'text_column' in record and isinstance(record['text_column'], str):
        text_to_analyze = record['text_column']
    else: # Fallback: try to find the first string field to analyze
        for value in record.values():
            if isinstance(value, str):
                text_to_analyze = value
                # logger.debug(f"Used first string value as text_to_analyze for record {index}")
                break

    if not text_to_analyze:
        logger.warning(f"Could not find suitable text field for insight generation in record {index}. Searched for '{TEXT_COLUMN_NAME}', 'text_content', 'text_column', or first string.")
    return text_to_analyze


def lambda_handler(event, context):
    """
    Main Lambda function triggered by an S3 event.
    Processes the uploaded file, generates AI insights based on PROMPT_STRATEGY, and saves the augmented dataset.
    """
    logger.info(f"Received event: {json.dumps(event)}")
    logger.info(f"Using prompt strategy: {PROMPT_STRATEGY}")

    if not OPENAI_API_KEY or not OUTPUT_BUCKET_NAME:
        logger.error("Core envirnoment variables not configured.")
        return {'statusCode': 500, 'body': json.dumps({'error': 'OpenAI API key not configured.'})}
    if OUTPUT_FORMAT not in ['json', 'csv', 'parquet'] or not PYARROW_AVAILABLE:
        logger.error(f"Unsupported OUTPUT_FORMAT: {OUTPUT_FORMAT}.")
        return {'statusCode': 400, 'body': json.dumps({'error': f"Unsupported OUTPUT_FORMAT: {OUTPUT_FORMAT}"})}
    if PROMPT_STRATEGY not in ['per_record', 'summarize_all']:
        logger.error(f"Unsupported PROMPT_STRATEGY: {PROMPT_STRATEGY}.")
        return {'statusCode': 400, 'body': json.dumps({'error': f"Unsupported PROMPT_STRATEGY: {PROMPT_STRATEGY}"})}

    try:
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        logger.info(f"Processing file: s3://{bucket_name}/{object_key}")
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content_bytes = s3_object['Body'].read()
        original_data = process_data(file_content_bytes, object_key)
        logger.info(f"Successfully processed {len(original_data)} records from input file.")

        if not original_data:
            logger.warning("No data processed from the input file.")
            return {'statusCode': 200, 'body': json.dumps({'message': 'No data to process.'})}

        # 2. Generate insights using OpenAI based on PROMPT_STRATEGY
        augmented_data = [record.copy() for record in original_data]

        if PROMPT_STRATEGY == 'per_record':
            for index, record in enumerate(original_data):
                text_to_analyze = get_text_from_record(record, index)
                if text_to_analyze:
                    prompt = f"Provide a concise analysis or key insight for the following data point: \"{text_to_analyze}\""
                    try:
                        if index < len(augmented_data):
                           augmented_data[index]['ai_insight'] = get_openai_insights(prompt, OPENAI_API_KEY)
                    except Exception as e:
                        logger.error(f"Error getting OpenAI insight for record {index}: {e}")
                        if index < len(augmented_data):
                           augmented_data[index]['ai_insight'] = f"Error: {str(e)}"
                else:
                    if index < len(augmented_data):
                       augmented_data[index]['ai_insight'] = 'N/A - No suitable text field found'
            logger.info(f"Generated insights per record for {len(original_data)} records.")

        elif PROMPT_STRATEGY == 'summarize_all':
            texts_for_summary_list = []
            current_char_count = 0

            # Estimate overhead for "Record X: " and newline characters
            for i, record in enumerate(original_data[:MAX_RECORDS_FOR_SUMMARY]):
                text = get_text_from_record(record, i)
                if text:
                    # Construct the entry string that would be appended
                    entry_prefix = f"Record {i+1}: "
                    entry_text = text
                    full_entry = entry_prefix + entry_text

                    # Calculate length to add: len(full_entry) + 1 for newline (if not first item)
                    chars_to_add = len(full_entry) + (1 if texts_for_summary_list else 0)

                    if current_char_count + chars_to_add <= MAX_CHARS_FOR_SUMMARY_PROMPT:
                        texts_for_summary_list.append(full_entry)
                        current_char_count += chars_to_add
                    else:
                        # If adding the current full_entry would exceed, try adding a truncated part of it
                        if not texts_for_summary_list:
                             # Calculate remaining space for the text part of the entry
                            remaining_space_for_text = MAX_CHARS_FOR_SUMMARY_PROMPT - (len(entry_prefix) + (1 if texts_for_summary_list else 0))
                            if remaining_space_for_text > 0:
                                texts_for_summary_list.append(entry_prefix + entry_text[:remaining_space_for_text])
                                current_char_count += len(entry_prefix) + remaining_space_for_text + (1 if texts_for_summary_list else 0)
                                logger.warning(f"First text record for 'summarize_all' was truncated to fit MAX_CHARS_FOR_SUMMARY_PROMPT.")
                            # If no space even for prefix and some text, texts_for_summary_list will remain empty.
                        else:
                            logger.warning(f"Stopped adding records to 'summarize_all' prompt at record {i} to stay within {MAX_CHARS_FOR_SUMMARY_PROMPT} character limit. Processed {len(texts_for_summary_list)} records for summary.")
                        break

            if texts_for_summary_list:
                combined_text = "\n".join(texts_for_summary_list)
                if len(combined_text) > MAX_CHARS_FOR_SUMMARY_PROMPT:
                    combined_text = combined_text[:MAX_CHARS_FOR_SUMMARY_PROMPT]
                    logger.warning(f"Combined text for 'summarize_all' was truncated to {MAX_CHARS_FOR_SUMMARY_PROMPT} characters.")

                summary_prompt = (
                    f"The following are multiple data records. "
                    f"Please provide a single, comprehensive summary of the key themes, trends, or overall insights found across all these records. "
                    f"Do not analyze each record individually in your response; provide one holistic summary.\n\n"
                    f"Data Records:\n{combined_text}"
                )

                try:
                    overall_insight = get_openai_insights(summary_prompt, OPENAI_API_KEY)
                    logger.info(f"Generated overall insight for 'summarize_all' strategy.")
                    # Apply this single insight to all records, a new key in each record.
                    for record in augmented_data:
                        record['ai_overall_summary_insight'] = overall_insight
                except Exception as e:
                    logger.error(f"Error getting OpenAI insight for 'summarize_all': {e}")
                    for record in augmented_data:
                        record['ai_overall_summary_insight'] = f"Error generating summary: {str(e)}"
            else:
                logger.warning("No text found in records to generate 'summarize_all' insight.")
                for record in augmented_data:
                    record['ai_overall_summary_insight'] = "N/A - No text to summarize"

        # 3. Save the augmented dataset
        base_name = os.path.splitext(os.path.basename(object_key))[0]
        augmented_file_key = f"augmented_datasets/augmented_{base_name}.{OUTPUT_FORMAT}"
        output_body = None
        content_type = 'application/octet-stream'

        if OUTPUT_FORMAT == 'json':
            output_body = json.dumps(augmented_data, indent=2)
            content_type = 'application/json'
        elif OUTPUT_FORMAT == 'csv':
            if not augmented_data:
                output_body = ""
            else:
                headers = augmented_data[0].keys() if augmented_data else []
                csv_buffer = io.StringIO()
                writer = csv.DictWriter(csv_buffer, fieldnames=headers, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(augmented_data)
                output_body = csv_buffer.getvalue()
            content_type = 'text/csv'
        elif OUTPUT_FORMAT == 'parquet':
            if not augmented_data:
                 output_body = b''
            else:
                try:
                    table = pa.Table.from_pylist(augmented_data)
                    buffer = io.BytesIO()
                    pq.write_table(table, buffer)
                    output_body = buffer.getvalue()
                except Exception as e:
                    logger.error(f"Error converting augmented data to Parquet: {e}", exc_info=True)
                    return {'statusCode': 500, 'body': json.dumps({'error': f"Failed to prepare Parquet output: {str(e)}"}) }
            content_type = 'application/vnd.apache.parquet'

        if output_body is not None:
            try:
                s3_client.put_object(
                    Bucket=OUTPUT_BUCKET_NAME,
                    Key=augmented_file_key,
                    Body=output_body.encode('utf-8') if isinstance(output_body, str) else output_body,
                    ContentType=content_type
                )
                logger.info(f"Augmented dataset saved to s3://{OUTPUT_BUCKET_NAME}/{augmented_file_key}")
            except Exception as e:
                logger.error(f"Error saving augmented dataset to S3: {e}", exc_info=True)
                return {'statusCode': 500, 'body': json.dumps({'error': f"Failed to save augmented dataset: {str(e)}"}) }
        else:
             logger.error(f"Output body was not generated for format {OUTPUT_FORMAT}.")
             return {'statusCode': 500, 'body': json.dumps({'error': f"Failed to generate output for format {OUTPUT_FORMAT}"}) }

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Dataset processed and augmented successfully.',
                'augmented_file_location': f"s3://{OUTPUT_BUCKET_NAME}/{augmented_file_key}",
                'prompt_strategy_used': PROMPT_STRATEGY
            })
        }

    except Exception as e:
        logger.error(f"Unhandled error in lambda_handler: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"An unexpected error occurred: {str(e)}"})
        }
