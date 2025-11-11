from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from typing import List
from app.db import Base, engine, get_db
from app import models, schemas, crud
from sqlalchemy.orm import Session
from fastapi import HTTPException
from app.ingest_service import FarmIngestService
from app.ingest_sources import CsvSource, GeoJSONSource, XmlSource
import xml.etree.ElementTree as ET
from app.deps import get_ingest_service


app = FastAPI(title="Farms API (SQLite)")

# Create tables at startup
@app.on_event("startup")
def _init_db():
    Base.metadata.create_all(bind=engine)

@app.get("/farms", response_model=List[schemas.FarmOut])
def list_farms(db: Session = Depends(get_db)):
    return db.query(models.Farm).all()

@app.get("/farms/within")
def farms_within(
    lat: float,
    lon: float,
    radius: float = 50.0,
    db: Session = Depends(get_db),
):
    res = crud.farms_within_radius(db, lat, lon, radius)
    return [{"farm": schemas.FarmOut.model_validate(f), "distance_km": d} for f, d in res]

@app.get("/farms/{farm_id}", response_model=schemas.FarmOut)
def get_farm(farm_id: str, db: Session = Depends(get_db)):
    obj = db.get(models.Farm, farm_id)
    if not obj:
        raise HTTPException(404, "Farm not found")
    return obj

@app.post("/ingest/csv")
async def ingest_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    svc: FarmIngestService = Depends(get_ingest_service),
):
    try:
        content = (await file.read()).decode("utf-8")
        return svc.ingest(CsvSource(content), db)
    except UnicodeDecodeError as e:
        raise HTTPException(status_code=400, detail=f"CSV must be UTF-8 encoded: {e}")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error while ingesting CSV: {e}")

@app.post("/ingest/geojson")
async def ingest_geojson(
    geojson: dict,
    db: Session = Depends(get_db),
    svc: FarmIngestService = Depends(get_ingest_service),
):
    try:
        return svc.ingest(GeoJSONSource(geojson), db)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

# @app.post("/ingest/xml")
# async def ingest_xml(
#     file: UploadFile = File(...),
#     db: Session = Depends(get_db),
#     svc: FarmIngestService = Depends(get_ingest_service),
# ):
#     try:
#         content = (await file.read()).decode("utf-8")
#         return svc.ingest(XmlSource(content), db)
#     except UnicodeDecodeError as e:
#         raise HTTPException(status_code=400, detail=f"XML must be UTF-8 encoded: {e}")
#     except ET.ParseError as e:
#         raise HTTPException(status_code=422, detail=f"Invalid XML: {e}")
#     except ValueError as e:
#         # bubbles up schema/validation issues raised by XmlSource.records()
#         raise HTTPException(status_code=422, detail=str(e))
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Unexpected error while ingesting XML: {e}")