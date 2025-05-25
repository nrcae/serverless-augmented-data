# serialization.py
# Utilities for data serialization.

import json
import csv
import io

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = pq = None

def serialize_output(data, fmt):
    """Serialize data for the suggested output format."""

    if fmt == 'json':
        return json.dumps(data, indent=2), 'application/json'
    
    elif fmt == 'csv':
        if not data:
            return "", 'text/csv'
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=data[0].keys(), extrasaction='ignore')
        writer.writeheader()
        writer.writerows(data)
        return buf.getvalue(), 'text/csv'
    
    elif fmt == 'parquet' and pa and pq:
        if not data:
            return b'', 'application/vnd.apache.parquet'
        table = pa.Table.from_pylist(data)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        return buf.getvalue(), 'application/vnd.apache.parquet'
    return None, None
