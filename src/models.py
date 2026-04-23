from pydantic import BaseModel, Field
from typing import Literal, Optional
from bson import ObjectId

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

    class Config:
        arbitrary_types_allowed = True

    id: ObjectId = Field(alias="_id")
    timestamp: float
    duration_ms: int
    filepath_raw: str
    filepath_clean: str
    filepath_features: str
    labels: LabelsData


