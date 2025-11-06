# models.py
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Float, DateTime, JSON
from datetime import datetime, timezone
from app.db import Base

def utcnow():
    return datetime.now(timezone.utc)

class Farm(Base):
    __tablename__ = "farms"
    farm_id: Mapped[str] = mapped_column(String, primary_key=True)
    farm_name: Mapped[str | None] = mapped_column(String, nullable=True)
    acreage: Mapped[float | None] = mapped_column(Float, nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    geometry: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    source: Mapped[str | None] = mapped_column(String, nullable=True)
    last_updated: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
