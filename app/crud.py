# app/crud.py
from sqlalchemy.orm import Session
from datetime import datetime, timezone
from math import radians, sin, cos, atan2, sqrt
from . import models, schemas

EARTH_KM = 6371.0

def _haversine_km(lat1, lon1, lat2, lon2) -> float:
    if None in (lat1, lon1, lat2, lon2):
        return 0.0  # can't compare; treat as same place to avoid overwrite
    φ1, λ1, φ2, λ2 = map(radians, (lat1, lon1, lat2, lon2))
    dφ, dλ = φ2-φ1, λ2-λ1
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return 2 * EARTH_KM * atan2(sqrt(a), sqrt(1-a))

def upsert_farm(db: Session, data: schemas.FarmBase) -> models.Farm:
    # ensure tz-aware
    ts = data.last_updated if data.last_updated.tzinfo else data.last_updated.replace(tzinfo=timezone.utc)

    obj = db.get(models.Farm, data.farm_id)
    if obj is None:
        obj = models.Farm(
            farm_id=data.farm_id,
            farm_name=data.farm_name,
            acreage=data.acreage,
            geometry=data.geometry,
            latitude=data.latitude,
            longitude=data.longitude,
            source=data.source,
            last_updated=ts,
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj

    # update rules (newer wins for name/acreage when provided)
    if data.last_updated >= obj.last_updated:
        if data.farm_name:
            obj.farm_name = data.farm_name
        if data.acreage is not None:
            obj.acreage = data.acreage

        # geometry merge rule (5 km threshold)
        if data.latitude is not None and data.longitude is not None:
            dist = _haversine_km(obj.latitude, obj.longitude, data.latitude, data.longitude)
            if dist <= 5.0:
                obj.latitude, obj.longitude = data.latitude, data.longitude
                obj.geometry = data.geometry or obj.geometry
            else:
                # significant change: log/flag; here we just append a note in source
                obj.source = f"{obj.source};flagged-geo-shift"
        obj.last_updated = ts
        obj.source = data.source

    db.commit()
    db.refresh(obj)
    return obj

def farms_within_radius(db: Session, lat: float, lon: float, radius_km: float):
    # naive scan (SQLite); for larger data, add RTree/SpatiaLite later
    q = db.query(models.Farm).filter(models.Farm.latitude.isnot(None), models.Farm.longitude.isnot(None))
    results = []
    for f in q:
        d = _haversine_km(lat, lon, f.latitude, f.longitude)
        if d <= radius_km:
            results.append((f, d))
    return sorted(results, key=lambda x: x[1])
