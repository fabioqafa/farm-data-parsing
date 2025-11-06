from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from typing import List
from datetime import datetime, timezone
import csv, io, json
import pandas as pd  # add this import
from app.db import Base, engine, get_db
from app import models, schemas, crud
from sqlalchemy.orm import Session

app = FastAPI(title="Farms API (SQLite)")

def to_aware_utc(v) -> datetime:
    """
    Accepts str | datetime | None and returns an aware UTC datetime.
    """
    if v is None:
        return datetime.now(timezone.utc)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    # strings -> parse and force UTC
    # handles '2025-11-06T19:00:00Z' or '2025-11-06 19:00:00' etc.
    return pd.to_datetime(v, errors="coerce", utc=True).to_pydatetime()

# Create tables at startup (simple mode)
@app.on_event("startup")
def _init_db():
    Base.metadata.create_all(bind=engine)

@app.get("/farms", response_model=List[schemas.FarmOut])
def list_farms(db: Session = Depends(get_db)):
    return db.query(models.Farm).all()

@app.get("/farms/{farm_id}", response_model=schemas.FarmOut)
def get_farm(farm_id: str, db: Session = Depends(get_db)):
    obj = db.get(models.Farm, farm_id)
    if not obj:
        raise HTTPException(404, "Farm not found")
    return obj

@app.get("/farms/within")
def farms_within(lat: float, lon: float, radius: float = 50.0, db: Session = Depends(get_db)):
    res = crud.farms_within_radius(db, lat, lon, radius)
    return [{"farm": schemas.FarmOut.model_validate(f), "distance_km": d} for f, d in res]

# CSV ingest
@app.post("/ingest/csv")
async def ingest_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    content = (await file.read()).decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    count = 0
    for row in reader:
        # optional geometry from CSV (expects a JSON string)
        geom = None
        gval = row.get("geometry")
        if gval:
            try:
                geom = json.loads(gval)
            except Exception:
                geom = None  # ignore bad geometry strings

        payload = schemas.FarmBase(
            farm_id=str(row["farm_id"]),
            farm_name=row.get("farm_name") or None,
            acreage=float(row["acreage"]) if row.get("acreage") else None,
            latitude=float(row["latitude"]) if row.get("latitude") else None,
            longitude=float(row["longitude"]) if row.get("longitude") else None,
            geometry=geom,                 # <- picked up from CSV if present
            source="csv",
            last_updated=to_aware_utc(row.get("last_updated")),
        )
        crud.upsert_farm(db, payload)
        count += 1
    return {"ingested": count}

# GeoJSON ingest (body upload)
@app.post("/ingest/geojson")
async def ingest_geojson(geojson: dict, db: Session = Depends(get_db)):
    features = geojson.get("features", [])
    count = 0
    for feat in features:
        props = feat.get("properties", {})
        geom = feat.get("geometry")
        lat = lon = None
        if geom and geom.get("type") == "Point":
            lon, lat = geom.get("coordinates", [None, None])
        payload = schemas.FarmBase(
            farm_id=props["farm_id"],
            farm_name=props.get("farm_name"),
            acreage=float(props["acreage"]) if props.get("acreage") is not None else None,
            latitude=lat, longitude=lon,
            geometry=geom,
            source="geojson",
            last_updated=datetime.now(timezone.utc),
        )
        crud.upsert_farm(db, payload)
        count += 1
    return {"ingested": count}
