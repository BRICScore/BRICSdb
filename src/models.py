from pydantic import BaseModel
from typing import Optional

class MeasurementMetadata(BaseModel):
    measurement_id: str
    person_id: str
    timestamp: float
    duration_ms: int
    measurement_file_path: str
    labels: list[str]