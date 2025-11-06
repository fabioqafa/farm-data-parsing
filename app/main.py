from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from typing import List
from datetime import datetime, timezone
import csv, io, json
import pandas as pd  # add this import
from app.db import Base, engine, get_db
from app import models, schemas, crud
from sqlalchemy.orm import Session
from fastapi import HTTPException

app = FastAPI(title="Farms API (SQLite)")

def round4(v):
    return None if v is None else round(float(v), 4)


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
    flags = []
    ingestion_ts = datetime.now(timezone.utc)

    for row in reader:
        lat = float(row["latitude"]) if row.get("latitude") else None
        lon = float(row["longitude"]) if row.get("longitude") else None
        geom = json.loads(row["geometry"]) if row.get("geometry") else None

        payload = schemas.FarmBase(
            farm_id=str(row["farm_id"]),
            farm_name=row.get("farm_name") or None,
            acreage=float(row["acreage"]) if row.get("acreage") else None,
            latitude=round4(lat),
            longitude=round4(lon),
            geometry=geom,
            source="csv",
            last_updated=to_aware_utc(row.get("last_updated")),
        )
        _, gflag, reason = crud.upsert_farm(db, payload, ingestion_ts=ingestion_ts)
        if gflag:
            flags.append({"farm_id": payload.farm_id, "reason": reason})
        count += 1

    return {"ingested": count, "geometry_flags": flags}

@app.post("/ingest/geojson")
async def ingest_geojson(geojson: dict, db: Session = Depends(get_db)):
    gtype = geojson.get("type")
    if gtype == "FeatureCollection":
        features = geojson.get("features", []) or []
    elif gtype == "Feature":
        features = [geojson]
    else:
        raise HTTPException(status_code=422, detail="Body must be GeoJSON Feature or FeatureCollection")

    count = 0
    flags = []
    ingestion_ts = datetime.now(timezone.utc)

    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")

        lat = lon = None
        if geom:
            rep = crud.representative_point_from_geometry(geom)  # (lat, lon)
            if rep:
                lat, lon = rep
                lat, lon = round4(lat), round4(lon)  # ✅ clamp to 4 dp

        payload = schemas.FarmBase(
            farm_id=str(props["farm_id"]),
            farm_name=props.get("farm_name") or None,
            acreage=float(props["acreage"]) if props.get("acreage") not in (None, "") else None,
            latitude=lat,
            longitude=lon,
            geometry=geom or None,
            source="geojson",
            last_updated=to_aware_utc(props.get("last_updated")),
        )
        _, gflag, reason = crud.upsert_farm(db, payload, ingestion_ts=ingestion_ts)
        if gflag:
            flags.append({"farm_id": payload.farm_id, "reason": reason})
        count += 1

    return {"ingested": count, "geometry_flags": flags}
