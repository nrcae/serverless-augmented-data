# data_utils.py
# Utilities for processing input data and augmenting it with insights.

import csv
import io
import json
import logging
try:
    import pyarrow.parquet as pq
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def process_data(file_content_bytes: bytes, file_key: str) -> list[dict]:
    """
    Processes raw file content into a list of dictionaries.
    Determines file type from the file_key extension.
    Supports CSV, JSON, TXT, and Parquet (if pyarrow is installed).

    Args:
        file_content_bytes (bytes): The raw byte content of the file.
        file_key (str): The S3 object key (filename) to infer file type.

    Returns:
        list[dict]: A list of records, where each record is a dictionary.
                    Returns an empty list if processing fails or content is empty.
    """
    records = []
    if not file_content_bytes:
        logger.warning("File content is empty. Returning no records.")
        return records

    file_type = file_key.split('.')[-1].lower()
    logger.info(f"Attempting to process file with inferred type: {file_type}")

    try:
        if file_type == 'csv':
            # Decode bytes to string for CSV processing
            file_content_str = file_content_bytes.decode('utf-8')
            csvfile = io.StringIO(file_content_str)
            reader = csv.DictReader(csvfile)
            for row in reader:
                records.append(dict(row))
            logger.info(f"Successfully processed {len(records)} records from CSV content.")

        elif file_type == 'json':
            # Decode bytes to string for JSON processing
            file_content_str = file_content_bytes.decode('utf-8')
            data = json.loads(file_content_str)
            if isinstance(data, list): # Expecting a list of objects
                if all(isinstance(item, dict) for item in data):
                    records = data
                else:
                    logger.error("JSON is a list, but not all items are objects.")
                    return []
            elif isinstance(data, dict): # Or a single object
                records = [data]
            else: # Or a JSON Lines format (each line is a JSON object)
                logger.info("Attempting to process as JSON Lines format.")
                file_content_str_lines = io.StringIO(file_content_str)
                for line in file_content_str_lines:
                    line = line.strip()
                    if line:
                        records.append(json.loads(line))
            logger.info(f"Successfully processed {len(records)} records from JSON content.")

        elif file_type == 'txt':
            # Decode bytes to string for TXT processing
            file_content_str = file_content_bytes.decode('utf-8')
            # Treat each non-empty line as a separate piece of text for analysis
            # Alternatively, could treat the whole file as one document.
            lines = [line.strip() for line in file_content_str.splitlines() if line.strip()]
            for i, line_content in enumerate(lines):
                records.append({'line_number': i + 1, 'text_content': line_content})
            if not records and file_content_str.strip(): # Handle case of single block of text with no newlines
                 records.append({'line_number': 1, 'text_content': file_content_str.strip()})
            logger.info(f"Successfully processed {len(records)} records from TXT content.")

        elif file_type == 'parquet':
            if not PYARROW_AVAILABLE:
                logger.error("PyArrow library is not available. Cannot process Parquet files.")
                raise ImportError("PyArrow is required for Parquet processing but not installed.")

            buffer = io.BytesIO(file_content_bytes)
            table = pq.read_table(buffer)
            # Convert PyArrow Table to list of dictionaries
            records = table.to_pylist()
            logger.info(f"Successfully processed {len(records)} records from Parquet content.")

        else:
            logger.error(f"Unsupported file type: {file_type} for file: {file_key}")
            # Fallback: treat as raw binary if no specific parser, or return error
            return []

    except UnicodeDecodeError as e:
        logger.error(f"Encoding error processing file {file_key} as text: {e}. Ensure UTF-8 encoding or handle other encodings.")
        return []
    except csv.Error as e:
        logger.error(f"CSV processing error for {file_key}: {e}")
        return []
    except json.JSONDecodeError as e:
        logger.error(f"JSON processing error for {file_key}: {e}")
        return []
    except Exception as e: # Catch other specific library errors if they occur
        logger.error(f"Generic error in process_data for {file_key} (type: {file_type}): {e}", exc_info=True)
        return []
        
    return records


def augment_dataset(original_data: list[dict], insights: list[dict]) -> list[dict]:
    """
    Augments the original dataset with the generated insights.
    This basic version adds the insight as a new key to each corresponding record.

    Args:
        original_data (list[dict]): The original data, a list of records.
        insights (list[dict]): A list of insights, where each insight dict
                               is expected to have 'original_record_index' and 'insight'.

    Returns:
        list[dict]: The augmented dataset.
    """
    augmented_data = [record.copy() for record in original_data]

    insights_map = {item['original_record_index']: item['insight'] for item in insights if 'original_record_index' in item and 'insight' in item}

    for i, record in enumerate(augmented_data):
        if i in insights_map:
            record['ai_insight'] = insights_map[i]
        else:
            record['ai_insight'] = "N/A" 
            logger.warning(f"No insight found for original record index {i}. Marked as N/A.")
            
    logger.info(f"Augmented {len(augmented_data)} records.")
    return augmented_data

if __name__ == '__main__':
    # Example Usage for local testing (assuming UTF-8 for text files)

    # Test CSV
    sample_csv_content_bytes = "id,name,text_column\n1,itemA,Review for item A.\n2,itemB,Another review here.".encode('utf-8')
    print("Testing CSV processing...")
    processed_csv = process_data(sample_csv_content_bytes, "test.csv")
    print(f"Processed CSV: {processed_csv}")

    # Test JSON (list of objects)
    sample_json_list_bytes = '[{"id": 1, "text_column": "JSON text A"}, {"id": 2, "text_column": "JSON text B"}]'.encode('utf-8')
    print("\nTesting JSON (list) processing...")
    processed_json_list = process_data(sample_json_list_bytes, "test.json")
    print(f"Processed JSON (list): {processed_json_list}")

    # Test JSON (JSON Lines)
    sample_json_lines_bytes = '{"id": 1, "text_column": "JSON Line 1"}\n{"id": 2, "text_column": "JSON Line 2"}'.encode('utf-8')
    print("\nTesting JSON (lines) processing...")
    processed_json_lines = process_data(sample_json_lines_bytes, "test.jsonl")
    # To explicitly test json lines, the file type logic would need 'jsonl' or similar.
    # For now, assuming .json could be json lines.
    print(f"Processed JSON (lines): {processed_json_lines}")

    # Test TXT
    sample_txt_content_bytes = "First line of text.\nSecond line for analysis.\n\nFourth line (after blank).".encode('utf-8')
    print("\nTesting TXT processing...")
    processed_txt = process_data(sample_txt_content_bytes, "test.txt")
    print(f"Processed TXT: {processed_txt}")

    # Test Parquet (requires pyarrow and a sample parquet file)
    if PYARROW_AVAILABLE:
        print("\nTesting Parquet processing...")
        try:
            data = {'col1': [1, 2], 'text_column': ['parquet data A', 'parquet data B']}
            table = pa.Table.from_pydict(data)
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            sample_parquet_bytes = buffer.getvalue()

            processed_parquet = process_data(sample_parquet_bytes, "test.parquet")
            print(f"Processed Parquet: {processed_parquet}")
        except Exception as e:
            print(f"Error during Parquet local test: {e}")
    else:
        print("\nSkipping Parquet test as PyArrow is not available.")

    # Augmentation test (using CSV data as example)
    if processed_csv:
        sample_insights = [
            {'original_record_index': 0, 'insight': 'Insight for item A.'},
            {'original_record_index': 1, 'insight': 'Insight for item B.'}
        ]
        print("\nTesting augmentation...")
        augmented_result = augment_dataset(processed_csv, sample_insights)
        print(f"Augmented Data: {json.dumps(augmented_result, indent=2)}")
