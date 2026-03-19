from contextlib import asynccontextmanager
import json
import bson.objectid as bs
from fastapi import FastAPI, UploadFile, File, HTTPException, Form, Query
from starlette.background import BackgroundTask
from fastapi.responses import FileResponse, JSONResponse
from pathlib import Path
from utils import jsonl_to_bson, bson_to_jsonl, zip_directory
import tempfile
from db import connectToDB
import os
from typing import List, Optional
from models import *
import shutil
import dotenv
from pymongo.collection import Collection


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

@app.get("/test")
async def test():
    return {"status": "ok"}

@app.put("/measurement/upload")
async def uploadMeasurement(measurement_file_raw: UploadFile = File(...),
                            measurement_file_clean: UploadFile = File(...),
                            person_id: str = Form(...),
                            timestamp: float = Form(...),
                            duration_ms: int = Form(...),
                            labels: str = Form(...)):
    
    try:
        labels_dict = json.loads(labels)
        if not isinstance(labels_dict, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Labels must be a JSON array")
    
    try:
        labels_data = LabelsData(**labels_dict)
    except Exception:
        raise HTTPException(400, "Invalid label structure")
    
    measurement_id = bs.ObjectId()

    file_path_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
    file_path_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"


    metadata_dict = {
        "_id": measurement_id,
        "person_id": person_id,
        "timestamp": timestamp,
        "duration_ms": duration_ms,
        "measurement_file_path_raw": str(file_path_raw),
        "measurement_file_path_clean": str(file_path_clean),
        "labels": labels_data.model_dump()
    }

    metadata = MeasurementMetadata(**metadata_dict)

    measurement_coll: Collection = app.state.db.get_collection("measurement")
  
    try:      
        await jsonl_to_bson(measurement_file_raw.file, file_path_raw)
        await jsonl_to_bson(measurement_file_clean.file, file_path_clean)   
        await measurement_coll.insert_one(metadata.model_dump())
        
    except Exception:
        file_path_raw.unlink(missing_ok=True)
        file_path_clean.unlink(missing_ok=True)
        raise
                
    return JSONResponse(content=metadata.model_dump(), status_code=201)

@app.get("/measurement/download")
async def downloadMeasurements( person_id: Optional[str] = Query(None),  
                                length_min: Optional[int] = Query(0),
                                length_max: Optional[int] = Query(86400000),
                                age_min: Optional[int] = Query(0),
                                age_max: Optional[int] = Query(100),
                                level: Optional[List[str]] = Query(None),
                                gender: Optional[List[str]] = Query(None),
                                activity: Optional[List[str]] = Query(None),
                                condition: Optional[List[str]] = Query(None),
                                health: Optional[List[str]] = Query(None)):

    coll:Collection  = app.state.db.get_collection("measurement")

    query = {}

    if person_id:
        query["person_id"] = person_id
    query["duration_ms"] = {}
    query["duration_ms"]["$gte"] = length_min
    query["duration_ms"]["$lte"] = length_max
    
    query["labels"] = {}
    query["labels"]["$elemMatch"] = {}
    query["labels"]["$elemMatch"]["age"] = {"$gte": age_min, "$lte": age_max}
    if level:
        query["labels"]["$elemMatch"]["level"] = {"$in": level}
    if gender:
        query["labels"]["$elemMatch"]["gender"] = {"$in": gender}
    if activity:
        query["labels"]["$elemMatch"]["activity"] = {"$in": activity}
    if condition:
        query["labels"]["$elemMatch"]["condition"] = {"$in": condition}
    if health:
        query["labels"]["$elemMatch"]["health"] = {"$in": health}

    measurementIndexes = coll.find(query)

    tmp_dir = Path(tempfile.mkdtemp())
    dataset_dir = tmp_dir / "dataset"
    dataset_dir.mkdir()
    try:
        async for index in measurementIndexes:
            measurement = MeasurementMetadata(**index)
            if level == None or "clean" in level:
                temp_file_path = dataset_dir / Path(measurement.measurement_file_path_clean).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_clean), temp_file_path, measurement)
            if level == None or "raw" in level:
                temp_file_path = dataset_dir / Path(measurement.measurement_file_path_raw).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_raw), temp_file_path, measurement)
                
        zip_path = tmp_dir / "measurements_dataset.zip"
        zip_directory(dataset_dir, zip_path)

        return FileResponse(
            zip_path, filename=f"measurements_dataset.zip", media_type="application/zip", background=BackgroundTask(shutil.rmtree, tmp_dir)
        )
    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

@app.delete("/measurement/delete")
async def deleteMeasurement(measurement_id: str = Query(None)):
    coll: Collection = app.state.db.get_collection("measurement")

    file_path_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
    file_path_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"


    file_path_raw.unlink()
    file_path_clean.unlink()
    measurement = await coll.find_one_and_delete(measurement_id)

    return JSONResponse(content=measurement, status_code=200)


