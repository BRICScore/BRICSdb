from contextlib import asynccontextmanager
import json
import bson.objectid as bs
from fastapi import Depends, FastAPI, UploadFile, File, HTTPException, Form, Query
from fastapi.responses import FileResponse
from pathlib import Path
from utils import jsonl_to_bson, bson_to_jsonl, zip_directory
import tempfile
from db import connectToDB
import os
from typing import Optional
from models import MeasurementMetadata
import shutil
import dotenv
from pymongo.client_session import ClientSession



@asynccontextmanager
async def lifespan(app: FastAPI):
    dotenv.load_dotenv(".env")
    app.state.client = await connectToDB()
    app.state.FILE_PATH=Path(os.getenv("FILE_PATH"))
    app.state.FILE_PATH.mkdir(exist_ok=True)
    app.state.db = app.state.client.get_database("brics")
    yield
    await app.state.client.close()


app = FastAPI(title="BRICS API",lifespan=lifespan)

async def get_session():
    session = app.state.client.start_session()
    try:
        yield session
    finally:
        await session.end_session()

@app.get("/test")
async def test():
    return {"status": "ok"}

@app.put("/measurement/upload")
async def uploadMeasurement(measurement_file_raw: UploadFile = File(...),
                            measurement_file_work: UploadFile = File(...),
                            person_id: str = Form(...),
                            timestamp: float = Form(...),
                            duration_ms: int = Form(...),
                            labels: str = Form(...), session: ClientSession = Depends(get_session)):
    
    try:
        labels_list = json.loads(labels)
        if not isinstance(labels_list, list):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Labels must be a JSON list")
    
    measurement_id = bs.ObjectId()

    file_path_raw = app.state.FILE_PATH / (str(measurement_id) + "_raw.bson")
    file_path_work = app.state.FILE_PATH / (str(measurement_id) + "_work.bson")

    metadata_dict = {
        "_id": measurement_id,
        "person_id": person_id,
        "timestamp": timestamp,
        "duration_ms": duration_ms,
        "measurement_file_path_raw": str(file_path_raw),
        "measurement_file_path_work": str(file_path_work),
        "labels": labels_list
    }

    metadata = MeasurementMetadata(**metadata_dict)

    measurement_coll = app.state.db.get_collection("measurement")
  
    try:      
        await jsonl_to_bson(measurement_file_raw.file, file_path_raw)
        await jsonl_to_bson(measurement_file_work.file, file_path_work)   

        await session.start_transaction()
        await measurement_coll.insert_one(metadata.model_dump(), session=session)
        await session.commit_transaction()
    except:
        await session.abort_transaction()
        file_path_raw.unlink(missing_ok=True)
        file_path_work.unlink(missing_ok=True)
        raise
                
    return

@app.get("/measurement/download")
async def downloadMeasurements( person_id: Optional[str] = Query(None), 
                                labels: Optional[str] = Query(None), 
                                length_min: Optional[int] = Query(None),
                                length_max: Optional[int] = Query(None),
                                quality: Optional[str] = Query(None), session: ClientSession = Depends(get_session)):

    coll = app.state.db.get_collection("measurement")

    try:
        labels_list = json.loads(labels)
        if not isinstance(labels_list, list):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Labels must be a JSON list")

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

    async with session.start_transaction():
        measurementIndexes = coll.find(query,session=session)

    tmp_dir = Path(tempfile.mkdtemp())
    dataset_dir = tmp_dir / "dataset"
    dataset_dir.mkdir()
    try:
        async for index in measurementIndexes:
            measurement = MeasurementMetadata(**index)
            if quality == "work" or quality == None:
                temp_file_path = dataset_dir / Path(measurement.measurement_file_path_work).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_work), temp_file_path, measurement)
            if quality == "raw" or quality == None:
                temp_file_path = dataset_dir / Path(measurement.measurement_file_path_raw).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_raw), temp_file_path, measurement)
                
        zip_path = tmp_dir / "measurements_dataset.zip"
        zip_directory(dataset_dir, zip_path)

        return FileResponse(
            zip_path, filename=f"measurements_dataset.zip", media_type="application/zip", background=lambda: shutil.rmtree(tmp_dir)
        )
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise



