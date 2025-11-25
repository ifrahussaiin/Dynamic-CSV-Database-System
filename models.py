from sqlalchemy import Column, Integer, String, Text, DateTime, JSON, Boolean, Float
from datetime import datetime
from app.database import Base

class DatasetMetadata(Base):
    """Stores metadata about each uploaded CSV file"""
    __tablename__ = "dataset_metadata"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String(255), unique=True, index=True, nullable=False)
    original_filename = Column(String(500), nullable=False)
    upload_timestamp = Column(DateTime, default=datetime.utcnow)
    last_modified = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    row_count = Column(Integer, nullable=False)
    column_count = Column(Integer, nullable=False)
    file_hash = Column(String(64), unique=True, index=True)
    schema_definition = Column(JSON, nullable=False)
    has_missing_values = Column(Boolean, default=False)
    missing_value_report = Column(JSON)
    duplicate_count = Column(Integer, default=0)
    error_log = Column(JSON)
    description = Column(Text)

class DataRow(Base):
    """Generic storage for all CSV data rows"""
    __tablename__ = "data_rows"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, index=True, nullable=False)
    row_number = Column(Integer, nullable=False)
    data_json = Column(JSON, nullable=False)
    row_hash = Column(String(64), index=True)
    has_missing_values = Column(Boolean, default=False)
    is_duplicate = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class ColumnIndex(Base):
    """Index for fast column-based queries"""
    __tablename__ = "column_index"
    
    id = Column(Integer, primary_key=True, index=True)
    dataset_id = Column(Integer, index=True, nullable=False)
    column_name = Column(String(255), index=True, nullable=False)
    data_type = Column(String(50), nullable=False)
    distinct_count = Column(Integer)
    min_value = Column(String(500))
    max_value = Column(String(500))
    sample_values = Column(JSON)

