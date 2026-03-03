from pydantic import BaseModel
from typing import Optional

class MeasurementMetadata(BaseModel):
    measurement_id: str
    person_id: str
    timestamp: float
    duration_ms: int
    measurement_file_path_raw: str
    measurement_file_path_work: str
    labels: list[str]