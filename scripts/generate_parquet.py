#!/usr/bin/env python3
"""
Generate Parquet files from mockData.ts for DuckDB-WASM consumption.

Reads the TypeScript mockData file and exports tables as Parquet files
to be served from the web/public/data directory.
"""

import json
import re
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
MOCK_DATA_FILE = PROJECT_ROOT / "web" / "src" / "data" / "mockData.ts"
OUTPUT_DIR = PROJECT_ROOT / "web" / "public" / "data"


def extract_array_from_ts(content: str, var_name: str) -> list:
    """Extract a JavaScript array from TypeScript source using bracket counting."""
    # Find the start of the array
    pattern = rf'export const {var_name}[^=]*=\s*\['
    match = re.search(pattern, content)
    if not match:
        print(f"Warning: Could not find {var_name}")
        return []

    start_idx = match.end() - 1  # Position of opening bracket
    bracket_count = 0
    end_idx = start_idx

    for i, char in enumerate(content[start_idx:], start=start_idx):
        if char == '[':
            bracket_count += 1
        elif char == ']':
            bracket_count -= 1
            if bracket_count == 0:
                end_idx = i + 1
                break

    array_str = content[start_idx:end_idx]

    # Remove problematic control characters (preserve \n=0x0a, \r=0x0d, \t=0x09)
    array_str = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', ' ', array_str)
    # Note: Not stripping // comments since they may appear inside string values

    try:
        return json.loads(array_str)
    except json.JSONDecodeError as e:
        print(f"Error parsing {var_name}: {e}")
        # Try to show where the error is
        lines = array_str.split('\n')
        if hasattr(e, 'lineno') and e.lineno <= len(lines):
            print(f"  Near line {e.lineno}: {lines[e.lineno-1][:100]}...")
        return []


def flatten_array_fields(records: list, array_fields: list[str]) -> list:
    """Convert array fields to JSON strings for Parquet storage."""
    result = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            # Convert camelCase to snake_case
            snake_key = re.sub(r'([A-Z])', r'_\1', key).lower()
            if key in array_fields and isinstance(value, list):
                new_record[snake_key] = json.dumps(value)
            else:
                new_record[snake_key] = value
        result.append(new_record)
    return result


def to_snake_case(records: list) -> list:
    """Convert all keys from camelCase to snake_case."""
    result = []
    for record in records:
        new_record = {}
        for key, value in record.items():
            snake_key = re.sub(r'([A-Z])', r'_\1', key).lower()
            new_record[snake_key] = value
        result.append(new_record)
    return result


def write_parquet(records: list, output_path: Path, array_fields: list[str] = None):
    """Write records to a Parquet file."""
    if not records:
        print(f"Skipping {output_path.name}: no records")
        return

    if array_fields:
        records = flatten_array_fields(records, array_fields)
    else:
        records = to_snake_case(records)

    table = pa.Table.from_pylist(records)
    pq.write_table(table, output_path, compression='snappy')
    print(f"Written: {output_path.name} ({len(records)} records, {output_path.stat().st_size / 1024:.1f} KB)")


def main():
    print("Reading mockData.ts...")
    content = MOCK_DATA_FILE.read_text()

    # Create output directory
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Extract and write each table
    tables = {
        'studios': {'array_fields': ['franchiseIds']},
        'franchises': {'array_fields': ['filmIds']},
        'films': {'array_fields': []},
        'characters': {'array_fields': ['filmIds']},
        'genderByYear': {'array_fields': []},
        'genderByRole': {'array_fields': []},
        'topTalents': {'array_fields': ['studios']},
    }

    for var_name, config in tables.items():
        print(f"\nProcessing {var_name}...")
        records = extract_array_from_ts(content, var_name)

        # Convert to snake_case filename
        file_name = re.sub(r'([A-Z])', r'_\1', var_name).lower() + '.parquet'
        output_path = OUTPUT_DIR / file_name

        write_parquet(records, output_path, config.get('array_fields'))

    print(f"\nDone! Parquet files written to {OUTPUT_DIR}")

    # List all generated files
    print("\nGenerated files:")
    for f in sorted(OUTPUT_DIR.glob("*.parquet")):
        print(f"  {f.name}: {f.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    main()
