# ==================== app/schemas.py ====================
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class DatasetUploadResponse(BaseModel):
    status: str
    dataset_id: int
    dataset_name: str
    rows_inserted: int
    columns: int
    duplicate_rows: int
    has_missing_values: bool
    missing_value_summary: Dict[str, Any]
    errors_detected: int
    schema: Dict[str, str]


class DatasetInfo(BaseModel):
    id: int
    name: str
    filename: str
    uploaded: datetime
    rows: int
    columns: int
    has_missing_values: bool
    duplicates: int


class SchemaResponse(BaseModel):
    dataset_name: str
    schema: Dict[str, str]
    row_count: int
    column_count: int
    has_missing_values: bool
    missing_value_report: Optional[Dict[str, Any]]


class DataResponse(BaseModel):
    dataset_name: str
    total_rows: int
    returned_rows: int
    data: List[Dict[str, Any]]
