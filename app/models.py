# app/models.py
from sqlalchemy import Column, String, Float, DateTime, JSON
from sqlalchemy.orm import validates
from datetime import datetime, timezone
from .db import Base

class Farm(Base):
    __tablename__ = "farms"

    farm_id = Column(String, primary_key=True, index=True)     # unique id per spec
    farm_name = Column(String, nullable=True)
    acreage = Column(Float, nullable=True)

    # store full GeoJSON or coordinates; simple JSON is fine for SQLite
    geometry = Column(JSON, nullable=True)

    # convenience columns for radius queries (Point centroid)
    latitude = Column(Float, nullable=True, index=True)
    longitude = Column(Float, nullable=True, index=True)

    source = Column(String, nullable=False)                    # "geojson" | "csv"
    last_updated = Column(DateTime(timezone=True), nullable=False, index=True)

    @validates("last_updated")
    def _tz(self, _, v):
        # ensure aware timestamps
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
