from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Query
from fastapi.responses import FileResponse
from pathlib import Path
from utils import jsonl_to_bson, bson_to_jsonl, zip_directory
import tempfile
from db import connectToDB
import uuid
import os
from typing import Optional
from models import MeasurementMetadata
import shutil
import dotenv

dotenv.load_dotenv(".env")
db = connectToDB()
FILE_PATH=Path(os.getenv("FILE_PATH"))
FILE_PATH.mkdir(exist_ok=True)

app = FastAPI(title="BRICS API")

@app.post("/measurement/upload")
async def uploadMeasurement(measurement_file: UploadFile = File(...),
                            person_id: str = Form(...),
                            timestamp: float = Form(...),
                            duration_ms: int = Form(...),
                            labels: list[str] = Form(...)):
    
    measurement_id = uuid.uuid4()

    file_path = FILE_PATH / str(measurement_id) + ".bson"

    metadata_dict = {
        "measurement_id": measurement_id,
        "person_id": person_id,
        "timestamp": timestamp,
        "duration_ms": duration_ms,
        "measurement_file_path": file_path,
        "labels": labels
    }

    metadata = MeasurementMetadata(**metadata_dict)

    coll = db.get_collection("measurement")

    result = await coll.insert_one(metadata.model_dump())

    if result.acknowledged:
        await jsonl_to_bson(measurement_file, file_path)

    return

@app.get("/measurement/download")
async def downloadMeasurements( person_id: Optional[str] = Query(None), 
                                labels: Optional[list[str]] = Query(None), 
                                length_min: Optional[int] = Query(None),
                                length_max: Optional[int] = Query(None)):

    coll = db.get_collection("measurement")

    query = {}

    if person_id:
        query["person_id"] = person_id
    if labels:
        query["labels"] = { "$all": labels }
    if length_min  or length_max:
        query["duration_ms"] = {}
        if length_min:
            query["duration_ms"]["$gte"] = length_min
        if length_max:
            query["duration_ms"]["$lte"] = length_max

    measurementIndexes = coll.find(query)

    tmp_dir = Path(tempfile.mkdtemp())
    dataset_dir = tmp_dir / "dataset"
    dataset_dir.mkdir()
    try:
        for index in measurementIndexes:
            measurement = MeasurementMetadata(**index)
            temp_file_path = dataset_dir / Path(measurement.measurement_file_path).name
            bson_to_jsonl(Path(measurement.measurement_file_path), temp_file_path, measurement)

        zip_path = tmp_dir / "measurements_dataset.zip"
        zip_directory(dataset_dir, zip_path)

        return FileResponse(
            zip_path, filename=f"measurements_dataset.zip", media_type="application/zip", background=lambda: shutil.rmtree(tmp_dir)
        )
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise



