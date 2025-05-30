# dynamodb.py
# Utilities for storing to dynamodb.

import logging
import boto3
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

MAX_STRING_FIELD_BYTES_FOR_TRUNCATION = 300 * 1024
# Truncate to a reasonable number of characters to avoid overly large items.
TRUNCATED_STRING_CHAR_LIMIT = 100000

def save_to_dynamodb(data: list, table_name: str, object_key: str, dynamodb=None):
    """
    Saves the augmented data to a DynamoDB table.
    Each record in the augmented_data list will be saved as a separate item.
    Adds a unique ID and a timestamp to each item.
    Handles potential DynamoDB item size limits by truncating large string fields.
    """
    if not table_name:
        logger.warning("DYNAMODB_TABLE_NAME is not set. Skipping DynamoDB save.")
        return

    if dynamodb is None:
        dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table(table_name)

    timestamp = datetime.utcnow().isoformat() + "Z" # ISO 8601 format with Z for UTC
    logger.info(f"Attempting to save {len(data)} records to DynamoDB table: {table_name}")

    unprocessed_items_count = 0
    saved_items_count = 0

    try:
        with table.batch_writer() as batch:
            for i, record in enumerate(data):
                try:
                    # Create a unique ID for each item based on the original object key and record index
                    item_id = f"{object_key.replace('/', '_')}_{i}"

                    # Convert non-serializable types and handle potential large strings.
                    dynamodb_item = {
                        'id': item_id,
                        'timestamp': timestamp,
                        'original_file_key': object_key,
                    }

                    # Add all key-value pairs from the record to the dynamodb_item
                    for key, value in record.items():
                        # DynamoDB doesn't support empty strings or empty sets.
                        if isinstance(value, str):
                            if not value:
                                continue
                            if len(value.encode('utf-8')) > MAX_STRING_FIELD_BYTES_FOR_TRUNCATION: # Roughly 300KB (limit 400k)
                                logger.warning(f"Truncating large string field '{key}' for item '{item_id}'.")
                                value = value[:100000] + "... (truncated)" # Truncate to first 100k chars
                        elif isinstance(value, (list, dict)):
                            pass
                        elif value is None:
                            continue

                        dynamodb_item[key] = value

                    batch.put_item(Item=dynamodb_item)
                    saved_items_count += 1
                    logger.debug(f"Successfully saved item {item_id} to DynamoDB.")

                except Exception as e:
                    unprocessed_items_count += 1
                    logger.error(f"Error saving record {i} (ID: {item_id}) to DynamoDB: {e}", exc_info=True)

    except boto3.exceptions.Boto3Error as e:
        logger.error(f"Boto3 specific error during DynamoDB batch save to '{table_name}': {e}", exc_info=True)
    except Exception as e:
        logger.error(f"General error during DynamoDB batch save to '{table_name}': {e}", exc_info=True)

    logger.info(f"Finished attempting to save records to DynamoDB.")