# serialization.py
# Utilities for data serialization.

import json
import csv
import io
import logging

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = pq = None

logger = logging.getLogger(__name__)

def serialize_output(data: list[dict], fmt: str, pyarrow_available: bool = False, pa_module=None, pq_module=None) -> tuple[str | bytes | None, str | None]:
    """
    Serialize data to the specified output format.

    Args:
        data (list[dict]): The list of dictionaries to serialize.
        fmt (str): The desired output format ('json', 'csv', 'parquet').
        pyarrow_available (bool): Flag indicating if pyarrow is available.
        pa_module: The pyarrow module (if available).
        pq_module: The pyarrow.parquet module (if available).

    Returns:
        tuple[str | bytes | None, str | None]: A tuple containing the serialized data
                                               (string for json/csv, bytes for parquet)
                                               and the content type string.
                                               Returns (None, None) or raises error on failure/unsupported format.
    """
    fmt = fmt.lower()

    if fmt == 'json':
        try:
            return json.dumps(data, indent=2), 'application/json'
        except TypeError as e:
            logger.error(f"JSON serialization error: {e}", exc_info=True)
            raise ValueError(f"Data not JSON serializable: {e}") from e

    elif fmt == 'csv':
        if not data:
            return "", 'text/csv'

        # Robust header generation: get all unique keys from all dicts
        all_keys = set()
        for row in data:
            if isinstance(row, dict):
                all_keys.update(row.keys())
            else:
                logger.warning(f"Encountered non-dict item in data for CSV serialization: {type(row)}")

        if not all_keys: # If data contained only non-dicts or empty dicts
             logger.warning("No keys found for CSV header generation.")
             return "", 'text/csv'

        headers = sorted(list(all_keys)) # Sort for consistent column order

        csv_buffer = io.StringIO()
        # extrasaction='ignore' to not raise error if a row is missing a header key
        # restval='' to write an empty string for missing values.
        writer = csv.DictWriter(csv_buffer, fieldnames=headers, extrasaction='ignore', restval='')
        writer.writeheader()
        try:
            writer.writerows(data)
        except Exception as e:
            logger.error(f"Error writing CSV rows: {e}", exc_info=True)
            raise ValueError(f"Error during CSV row writing: {e}") from e
        return csv_buffer.getvalue(), 'text/csv'

    elif fmt == 'parquet':
        if not pyarrow_available or not pa_module or not pq_module:
            logger.error("Parquet format requested, but PyArrow library (pa_module or pq_module) is not available/passed.")
            raise ImportError("PyArrow library is required for Parquet format but not available.")

        if not data:
            # For Parquet, an empty file is typically represented by b'' or a file with schema but no rows.
            return b'', 'application/vnd.apache.parquet'
        try:
            table = pa_module.Table.from_pylist(data)
            buffer = io.BytesIO()
            pq_module.write_table(table, buffer)
            return buffer.getvalue(), 'application/vnd.apache.parquet'
        except Exception as e:
            logger.error(f"Error converting data to Parquet or writing Parquet: {e}", exc_info=True)
            raise ValueError(f"Failed to prepare Parquet output: {e}") from e

    else:
        logger.error(f"Unsupported serialization format requested: {fmt}")
        raise ValueError(f"Unsupported serialization format: {fmt}")
