from pydantic import BaseModel, Field
from typing import Literal, Optional
import bson.objectid as bs

class BioData(BaseModel):
    person_id: str
    age: int = Field(..., ge=0, le=100)
    gender: Literal["male", "female"]
    health: str
    condition: Literal["sedentary", "regular", "active", "extreme"]
    weight: int
    height: int

class LabelsData(BaseModel):
    activity: str
    person_data: BioData

class MeasurementMetadata(BaseModel):
    _id: bs.ObjectId
    timestamp: float
    duration_ms: int
    filepath_raw: str
    filepath_clean: str
    filepath_features: str
    labels: LabelsData


