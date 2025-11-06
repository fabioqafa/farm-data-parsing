# app/schemas.py
from pydantic import BaseModel, Field
from typing import Optional, Any
from datetime import datetime

class FarmBase(BaseModel):
    farm_id: str
    farm_name: Optional[str] = None
    acreage: Optional[float] = None
    geometry: Optional[Any] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    source: str = Field(..., pattern="^(csv|geojson)$")
    last_updated: datetime

class FarmOut(FarmBase):
    class Config:
        from_attributes = True
