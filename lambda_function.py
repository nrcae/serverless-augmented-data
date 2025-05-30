# lambda_function.py
# Main AWS Lambda handler for the AI Dataset Insight Tool.

import json
import os
import boto3
import logging
from utils.openai import get_openai_insights
from utils.data import process_data, get_text_from_record
from utils.serialization import serialize_output
from utils.dynamodb import save_to_dynamodb
from config import OPENAI_API_KEY, OUTPUT_BUCKET_NAME, OUTPUT_FORMAT, PROMPT_STRATEGY, MAX_CHARS_FOR_SUMMARY_PROMPT, MAX_RECORDS_FOR_SUMMARY, DYNAMODB_TABLE_NAME

# For Parquet output, if supported
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 and DynamoDB client
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')


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
        logger.debug(f"Processing file: s3://{bucket_name}/{object_key}")
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
                    prompt = (
                        f"Analyze the following single data point and give a concise, actionable insight. "
                        f"Focus on key takeaways, potential implications, or notable observations. "
                        f"The insight should be no more than 2-3 sentences.\n\n"
                        f"Data Point: \"{text_to_analyze}\""
                    )
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
                if not text:
                    continue
                # Construct the entry string that would be appended
                entry_prefix = f"Record {i+1}: "
                full_entry = entry_prefix + text
                newline_cost = 1 if texts_for_summary_list else 0
                entry_length = len(full_entry) + newline_cost

                if current_char_count + entry_length <= MAX_CHARS_FOR_SUMMARY_PROMPT:
                    texts_for_summary_list.append(full_entry)
                    current_char_count += entry_length

                else:
                    # If adding the current full_entry would exceed, try adding a truncated part of it
                    if not texts_for_summary_list:
                        # Calculate remaining space for the text part of the entry
                        remaining_space_for_text = MAX_CHARS_FOR_SUMMARY_PROMPT - len(entry_prefix)
                        if remaining_space_for_text > 0:
                            truncated_entry = entry_prefix + text[:remaining_space_for_text]
                            texts_for_summary_list.append(truncated_entry)
                            current_char_count += len(truncated_entry)
                            logger.warning(f"First text record for 'summarize_all' was truncated to fit MAX_CHARS_FOR_SUMMARY_PROMPT.")
                    else:
                        logger.warning(f"Stopped adding records to 'summarize_all' prompt at record {i} "
                                       " to stay within {MAX_CHARS_FOR_SUMMARY_PROMPT} character limit. "
                                       " Processed {len(texts_for_summary_list)} records for summary.")
                    break

            if texts_for_summary_list:
                combined_text = "\n".join(texts_for_summary_list)
                if len(combined_text) > MAX_CHARS_FOR_SUMMARY_PROMPT:
                    combined_text = combined_text[:MAX_CHARS_FOR_SUMMARY_PROMPT]
                    logger.warning(f"Combined text for 'summarize_all' was truncated to {MAX_CHARS_FOR_SUMMARY_PROMPT} characters.")

                summary_prompt = (
                    f"You are an expert data analyst. The following is a collection of individual data records. "
                    f"Your task is to identify and synthesize the overarching themes, significant trends, "
                    f"and key insights present across *all* these records. "
                    f"Give a single, comprehensive summary that highlights the most important findings. "
                    f"Avoid analyzing each record separately. Structure your response as a cohesive narrative. "
                    f"Aim for a summary that is insightful and concise, ideally within 3-5 paragraphs.\n\n"
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

        output_body, content_type = serialize_output(augmented_data, OUTPUT_FORMAT)

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

        # 4. Save the augmented dataset to DynamoDB (if DYNAMODB_TABLE_NAME is set)
        if DYNAMODB_TABLE_NAME:
            try:
                save_to_dynamodb(augmented_data, DYNAMODB_TABLE_NAME, object_key, dynamodb)
                logger.info(f"Augmented dataset saved to DynamoDB table: {DYNAMODB_TABLE_NAME}")
            except Exception as e:
                logger.error(f"Error saving augmented dataset to DynamoDB: {e}", exc_info=True)
        else:
            logger.info("DYNAMODB_TABLE_NAME environment variable not set. Skipping DynamoDB save.")

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
