import hashlib
import json
import pandas as pd
from typing import Dict, Any, Tuple


def calculate_file_hash(content: bytes) -> str:
    """Calculate SHA-256 hash of file content"""
    return hashlib.sha256(content).hexdigest()


def calculate_row_hash(row_data: dict) -> str:
    """Calculate hash for a row to detect duplicates"""
    row_string = json.dumps(row_data, sort_keys=True)
    return hashlib.sha256(row_string.encode()).hexdigest()


def detect_missing_values(df: pd.DataFrame) -> Dict[str, Any]:
    """Detect and report missing values"""
    missing_report = {}
    has_missing = False

    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            has_missing = True
            missing_report[col] = {
                "count": int(null_count),
                "percentage": float(null_count / len(df) * 100),
                "positions": df[df[col].isnull()].index.tolist()[:100],
            }

    return {
        "has_missing": has_missing,
        "total_missing_cells": int(df.isnull().sum().sum()),
        "columns_with_missing": missing_report,
    }


def infer_schema(df: pd.DataFrame) -> Dict[str, str]:
    """Infer schema from DataFrame"""
    schema = {}
    for col in df.columns:
        dtype = str(df[col].dtype)
        if dtype.startswith("int"):
            schema[col] = "integer"
        elif dtype.startswith("float"):
            schema[col] = "float"
        elif dtype == "bool":
            schema[col] = "boolean"
        elif dtype == "datetime64[ns]":
            schema[col] = "datetime"
        else:
            schema[col] = "string"
    return schema


def validate_and_clean_data(df: pd.DataFrame) -> Tuple[pd.DataFrame, list]:
    """Validate data and detect errors"""
    errors = []

    # Check for completely empty rows
    empty_rows = df[df.isnull().all(axis=1)]
    if len(empty_rows) > 0:
        errors.append(
            {
                "type": "empty_rows",
                "count": len(empty_rows),
                "positions": empty_rows.index.tolist(),
            }
        )
        df = df.dropna(how="all")

    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        errors.append({"type": "duplicate_columns", "columns": duplicate_cols})
        df.columns = pd.io.parsers.base_parser.ParserBase(
            {"names": df.columns}
        )._maybe_dedup_names(df.columns)

    # Check for columns with all missing values
    all_null_cols = df.columns[df.isnull().all()].tolist()
    if all_null_cols:
        errors.append(
            {
                "type": "all_null_columns",
                "columns": all_null_cols,
                "action": "kept_for_schema_integrity",
            }
        )

    return df, errors
