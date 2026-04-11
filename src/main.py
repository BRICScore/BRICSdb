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
from typing import Any, List, Mapping, Optional
from models import *
import shutil
import dotenv
from pymongo.asynchronous.collection import AsyncCollection


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
                            measurement_file_features: UploadFile = File(...),
                            measurement_metadata: str = Form(...)):
    
    try:
        metadata_dict = json.loads(measurement_metadata)
        if not isinstance(metadata_dict, dict):
            raise ValueError()
    except Exception:
        raise HTTPException(400, "Metadata must be a JSON object")
    
    measurement_coll: AsyncCollection = app.state.db.get_collection("measurement")
    
    if "_id" not in metadata_dict or not metadata_dict["_id"]:
        raise HTTPException(400, "Missing ID from metadata")
    
    try:
        _id = bs.ObjectId(metadata_dict["_id"])
    except Exception:
        _id = bs.ObjectId()

    existing_measurement = await measurement_coll.find_one({"_id": _id})

    if existing_measurement:
        measurement_id = existing_measurement["_id"]
    else:
        measurement_id = _id

    try:
        file_path_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
        file_path_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"
        file_path_features: Path = app.state.FILE_PATH / f"{measurement_id}_features"
        metadata_dict["_id"] = measurement_id
        metadata_dict["measurement_file_path_raw"] = str(file_path_raw)
        metadata_dict["measurement_file_path_clean"] = str(file_path_clean)
        metadata_dict["measurement_file_path_features"] = str(file_path_features)
    except Exception:
        raise HTTPException(500, "Failed to prepare space for measurement")
    
    try:
        metadata = MeasurementMetadata(**metadata_dict)
    except Exception:
        raise HTTPException(400, "Invalid metadata structure")
  
    try:      
        await jsonl_to_bson(measurement_file_raw, file_path_raw)
        await jsonl_to_bson(measurement_file_clean, file_path_clean)
        await jsonl_to_bson(measurement_file_features, file_path_features)   
        await measurement_coll.update_one({"_id": measurement_id}, {"$set": metadata.model_dump()}, upsert=True)
        
    except Exception:
        file_path_raw.unlink(missing_ok=True)
        file_path_clean.unlink(missing_ok=True)
        file_path_features.unlink(missing_ok=True)
        raise
                
    return JSONResponse(content=metadata.model_dump(), status_code=201)

@app.get("/measurement/download")
async def downloadMeasurements( person_id: Optional[str] = Query(None),  
                                length_min: Optional[int] = Query(0),
                                length_max: Optional[int] = Query(86400000),
                                age_min: Optional[int] = Query(0),
                                age_max: Optional[int] = Query(100),
                                level: List[str] = Query(["raw", "clean", "features"]),
                                gender: Optional[List[str]] = Query(None),
                                activity: Optional[List[str]] = Query(None),
                                condition: Optional[List[str]] = Query(None),
                                health: Optional[List[str]] = Query(None),
                                weight_min: Optional[int] = Query(0),
                                weight_max: Optional[int] = Query(200),
                                height_min: Optional[int] = Query(0),
                                height_max: Optional[int] = Query(250)):

    coll: AsyncCollection  = app.state.db.get_collection("measurement")

    query: dict[str, Any] = {}

    if person_id:
        query["person_id"] = person_id
    query["duration_ms"] = {}
    query["duration_ms"]["$gte"] = length_min
    query["duration_ms"]["$lte"] = length_max
    query["weight"] = {}
    query["weight"]["$gte"] = weight_min
    query["weight"]["$lte"] = weight_max
    query["height"] = {}
    query["height"]["$gte"] = height_min
    query["height"]["$lte"] = height_max
    
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

    for type in level:
        (dataset_dir / type).mkdir(parents=True, exist_ok=True)
    try:
        async for index in measurementIndexes:
            measurement = MeasurementMetadata(**index)
            if "clean" in level:
                temp_file_path = dataset_dir / "clean" / Path(measurement.measurement_file_path_clean).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_clean), temp_file_path, measurement)
            if "raw" in level:
                temp_file_path = dataset_dir / "raw" / Path(measurement.measurement_file_path_raw).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_raw), temp_file_path, measurement)
            if "features" in level:
                temp_file_path = dataset_dir / "features" / Path(measurement.measurement_file_path_features).name
                await bson_to_jsonl(Path(measurement.measurement_file_path_features), temp_file_path, measurement)
                
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
    coll: AsyncCollection = app.state.db.get_collection("measurement")

    file_path_raw: Path = app.state.FILE_PATH / f"{measurement_id}_raw"
    file_path_clean: Path = app.state.FILE_PATH / f"{measurement_id}_clean"
    file_path_features: Path = app.state.FILE_PATH / f"{measurement_id}_features"

    measurement = await coll.find_one_and_delete({"_id": bs.ObjectId(measurement_id)})

    if measurement:
        file_path_raw.unlink(missing_ok=True)
        file_path_clean.unlink(missing_ok=True)
        file_path_features.unlink(missing_ok=True)

    return JSONResponse(content=measurement, status_code=200)


