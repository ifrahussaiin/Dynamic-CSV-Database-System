from sqlalchemy.orm import Session
from app.models import DatasetMetadata, DataRow, ColumnIndex
import pandas as pd
from app.utils import calculate_row_hash


def get_dataset_by_name(db: Session, dataset_name: str):
    """Get dataset by name"""
    return (
        db.query(DatasetMetadata)
        .filter(DatasetMetadata.dataset_name == dataset_name)
        .first()
    )


def get_dataset_by_hash(db: Session, file_hash: str):
    """Get dataset by file hash"""
    return (
        db.query(DatasetMetadata).filter(DatasetMetadata.file_hash == file_hash).first()
    )


def create_dataset_metadata(db: Session, metadata_data: dict):
    """Create new dataset metadata"""
    metadata = DatasetMetadata(**metadata_data)
    db.add(metadata)
    db.flush()
    return metadata


def insert_data_rows(db: Session, dataset_id: int, df: pd.DataFrame):
    """Insert data rows and return duplicate count"""
    row_hashes = set()
    duplicate_count = 0

    for idx, row in df.iterrows():
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}
        row_hash = calculate_row_hash(row_dict)
        is_duplicate = row_hash in row_hashes

        if is_duplicate:
            duplicate_count += 1

        row_hashes.add(row_hash)

        data_row = DataRow(
            dataset_id=dataset_id,
            row_number=idx,
            data_json=row_dict,
            row_hash=row_hash,
            has_missing_values=any(v is None for v in row_dict.values()),
            is_duplicate=is_duplicate,
        )
        db.add(data_row)

    return duplicate_count


def build_column_index(db: Session, dataset_id: int, df: pd.DataFrame):
    """Build column index for fast querying"""
    for col in df.columns:
        col_data = df[col].dropna()

        col_index = ColumnIndex(
            dataset_id=dataset_id,
            column_name=col,
            data_type=str(df[col].dtype),
            distinct_count=int(df[col].nunique()),
            min_value=str(col_data.min()) if len(col_data) > 0 else None,
            max_value=str(col_data.max()) if len(col_data) > 0 else None,
            sample_values=col_data.head(10).tolist() if len(col_data) > 0 else [],
        )
        db.add(col_index)


def delete_dataset_data(db: Session, dataset_id: int):
    """Delete all data associated with a dataset"""
    db.query(DataRow).filter(DataRow.dataset_id == dataset_id).delete()
    db.query(ColumnIndex).filter(ColumnIndex.dataset_id == dataset_id).delete()
