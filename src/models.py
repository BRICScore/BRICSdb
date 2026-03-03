from pydantic import BaseModel
from typing import Optional
import bson.objectid as bs

class MeasurementMetadata(BaseModel):
    _id: bs.ObjectId
    person_id: str
    timestamp: float
    duration_ms: int
    measurement_file_path_raw: str
    measurement_file_path_work: str
    labels: list[str]