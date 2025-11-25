from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from datetime import datetime
import pandas as pd
import io
import traceback
from typing import Optional

from app.database import get_db, init_db, engine
from app.models import DatasetMetadata, DataRow, Base
from app.schemas import DatasetUploadResponse, DatasetInfo, SchemaResponse, DataResponse
from app.utils import (
    calculate_file_hash,
    detect_missing_values,
    infer_schema,
    validate_and_clean_data,
)
from app.crud import (
    get_dataset_by_name,
    get_dataset_by_hash,
    create_dataset_metadata,
    insert_data_rows,
    build_column_index,
    delete_dataset_data,
)


# -----------------------------
# ✔ DATABASE CREATION FIX
# -----------------------------
def start_database():
    try:
        print("📌 Creating tables if not exist...")
        Base.metadata.create_all(bind=engine)
        print("✅ Database ready")
    except Exception as e:
        print("❌ DB Error:", e)


start_database()

app = FastAPI(
    title="Dynamic CSV Storage API",
    description="Upload and manage CSV with auto schema detection",
    version="1.0.0",
)


@app.get("/")
async def root():
    return {
        "status": "online",
        "api": "Dynamic CSV Storage API",
        "version": "1.0.0",
    }


# -------------------------------------------------
#               CSV UPLOAD FIXED VERSION
# -------------------------------------------------
@app.post("/upload-csv/", response_model=DatasetUploadResponse)
async def upload_csv(
    file: UploadFile = File(...),
    dataset_name: Optional[str] = None,
    description: Optional[str] = None,
    db: Session = Depends(get_db),
):
    try:
        print(f"📥 File received: {file.filename}")

        # Read bytes
        content = await file.read()

        # Hash
        file_hash = calculate_file_hash(content)

        # Duplicate file?
        match = get_dataset_by_hash(db, file_hash)
        if match:
            raise HTTPException(
                status_code=409,
                detail=f"Duplicate file. Already stored as: {match.dataset_name}",
            )

        # Safe CSV reading
        try:
            df = pd.read_csv(io.BytesIO(content), dtype=str, keep_default_na=False)
        except Exception:
            df = pd.read_csv(io.BytesIO(content), encoding="latin1", dtype=str)

        print(f"📊 Rows: {len(df)}, Cols: {len(df.columns)}")

        # Validate & clean
        df, errors = validate_and_clean_data(df)
        missing_report = detect_missing_values(df)
        schema = infer_schema(df)

        # Auto dataset name
        if not dataset_name:
            dataset_name = (
                file.filename.replace(".csv", "")
                + "_"
                + datetime.now().strftime("%Y%m%d_%H%M%S")
            )

        # Dataset name duplicate?
        if get_dataset_by_name(db, dataset_name):
            raise HTTPException(status_code=400, detail="Dataset name already exists")

        # Save metadata
        metadata_info = {
            "dataset_name": dataset_name,
            "original_filename": file.filename,
            "row_count": len(df),
            "column_count": len(df.columns),
            "file_hash": file_hash,
            "schema_definition": schema,
            "has_missing_values": missing_report["has_missing"],
            "missing_value_report": missing_report,
            "error_log": errors,
            "description": description,
        }

        metadata = create_dataset_metadata(db, metadata_info)

        # Insert data
        duplicate_count = insert_data_rows(db, metadata.id, df)
        metadata.duplicate_count = duplicate_count

        # Build Index
        build_column_index(db, metadata.id, df)

        db.commit()

        return {
            "status": "success",
            "dataset_id": metadata.id,
            "dataset_name": dataset_name,
            "rows_inserted": len(df),
            "columns": len(df.columns),
            "duplicate_rows": duplicate_count,
            "has_missing_values": missing_report["has_missing"],
            "errors_detected": len(errors),
            "schema": schema,
        }

    except Exception as e:
        db.rollback()

        print("\n" + "=" * 60)
        print("❌ ERROR OCCURRED:")
        print("Type:", type(e).__name__)
        print("Message:", str(e))
        print("TRACEBACK:")
        traceback.print_exc()
        print("=" * 60)

        raise HTTPException(status_code=500, detail=str(e))


# -----------------------------
# List Datasets
# -----------------------------
@app.get("/datasets/", response_model=list[DatasetInfo])
async def list_datasets(db: Session = Depends(get_db)):
    datasets = db.query(DatasetMetadata).all()
    return [
        {
            "id": d.id,
            "name": d.dataset_name,
            "filename": d.original_filename,
            "uploaded": d.upload_timestamp,
            "rows": d.row_count,
            "columns": d.column_count,
            "has_missing_values": d.has_missing_values,
            "duplicates": d.duplicate_count,
        }
        for d in datasets
    ]


# -----------------------------
# Get Schema
# -----------------------------
@app.get("/dataset/{dataset_name}/schema", response_model=SchemaResponse)
async def get_schema(dataset_name: str, db: Session = Depends(get_db)):
    dataset = get_dataset_by_name(db, dataset_name)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return {
        "dataset_name": dataset_name,
        "schema": dataset.schema_definition,
        "row_count": dataset.row_count,
        "column_count": dataset.column_count,
    }


# -----------------------------
# Get Data
# -----------------------------
@app.get("/dataset/{dataset_name}/data", response_model=DataResponse)
async def get_data(
    dataset_name: str,
    limit: int = 100,
    offset: int = 0,
    exclude_duplicates: bool = False,
    db: Session = Depends(get_db),
):
    dataset = get_dataset_by_name(db, dataset_name)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    query = db.query(DataRow).filter(DataRow.dataset_id == dataset.id)

    if exclude_duplicates:
        query = query.filter(DataRow.is_duplicate == False)

    rows = query.offset(offset).limit(limit).all()

    return {
        "dataset_name": dataset_name,
        "total_rows": dataset.row_count,
        "returned_rows": len(rows),
        "data": [r.data_json for r in rows],
    }


# -----------------------------
# Delete Dataset
# -----------------------------
@app.delete("/dataset/{dataset_name}")
async def delete_dataset(dataset_name: str, db: Session = Depends(get_db)):
    dataset = get_dataset_by_name(db, dataset_name)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    delete_dataset_data(db, dataset.id)
    db.delete(dataset)
    db.commit()

    return {"status": "deleted", "dataset_name": dataset_name}
