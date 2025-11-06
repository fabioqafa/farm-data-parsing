# crud.py
from datetime import datetime, timezone
from typing import Optional, Tuple
import math
from sqlalchemy.orm import Session
from app import models

# ----- helpers -----

def to_aware_utc(v) -> datetime:
    if v is None:
        return datetime.now(timezone.utc)
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088  # km
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def _flatten_coords(coords):
    if not isinstance(coords, (list, tuple)):
        return []
    if len(coords) == 2 and all(isinstance(v, (int, float)) for v in coords):
        return [coords]
    out = []
    for c in coords:
        out.extend(_flatten_coords(c))
    return out

def representative_point_from_geometry(geom: Optional[dict]) -> Optional[tuple[float, float]]:
    """
    Return (lat, lon) for a GeoJSON geometry:
      - Point: that point
      - Others: mean of all vertices
    """
    if not geom or not isinstance(geom, dict):
        return None
    gtype = geom.get("type")
    coords = geom.get("coordinates")

    if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        lon, lat = coords[0], coords[1]
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return (lat, lon)

    pts = _flatten_coords(coords)
    if not pts:
        return None
    lats = lons = n = 0
    for p in pts:
        if isinstance(p, (list, tuple)) and len(p) >= 2:
            lon, lat = p[0], p[1]
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                lats += lat; lons += lon; n += 1
    if n == 0:
        return None
    return (lats / n, lons / n)

# ----- main upsert with rules -----

def upsert_farm(
    db,
    payload,  # schemas.FarmBase
    *,
    ingestion_ts: Optional[datetime] = None,
    geom_diff_threshold_km: float = 5.0
) -> tuple[models.Farm, bool, Optional[str]]:
    """
    Returns (obj, geometry_flagged, reason)
    Rules:
      - Create if not exists
      - Update farm_name/acreage if incoming is newer AND value provided
      - Geometry: if rep-point shift > threshold -> flag (don't overwrite), else update
      - last_updated: ALWAYS set to ingestion_ts
    """
    ingestion_ts = to_aware_utc(ingestion_ts)
    obj = db.get(models.Farm, payload.farm_id)

    # INSERT
    if not obj:
        obj = models.Farm(
            farm_id=payload.farm_id,
            farm_name=payload.farm_name,
            acreage=payload.acreage,
            latitude=payload.latitude,
            longitude=payload.longitude,
            geometry=payload.geometry,
            source=payload.source,
            last_updated=ingestion_ts,
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj, False, None

    # UPDATE
    geometry_flagged = False
    flag_reason = None
    existing_ts = to_aware_utc(obj.last_updated)
    incoming_ts = to_aware_utc(payload.last_updated)

    # farm_name / acreage if newer
    if payload.farm_name and incoming_ts >= existing_ts:
        obj.farm_name = payload.farm_name
    if payload.acreage is not None and incoming_ts >= existing_ts:
        obj.acreage = payload.acreage

    # geometry merge
    if payload.geometry:
        new_pt = representative_point_from_geometry(payload.geometry)
        old_pt = representative_point_from_geometry(obj.geometry) if obj.geometry else None

        if old_pt and new_pt:
            d_km = haversine_km(old_pt[0], old_pt[1], new_pt[0], new_pt[1])
            if d_km > geom_diff_threshold_km:
                geometry_flagged = True
                flag_reason = f"Geometry shift {d_km:.2f} km > {geom_diff_threshold_km} km"
            else:
                obj.geometry = payload.geometry
                if payload.geometry.get("type") == "Point":
                    lon, lat = payload.geometry["coordinates"][:2]
                    obj.latitude = float(lat)
                    obj.longitude = float(lon)
        else:
            # if no previous (or cannot compute), accept incoming
            obj.geometry = payload.geometry
            if payload.geometry.get("type") == "Point":
                lon, lat = payload.geometry["coordinates"][:2]
                obj.latitude = float(lat)
                obj.longitude = float(lon)

    # allow direct lat/lon if provided
    if payload.latitude is not None:
        obj.latitude = payload.latitude
    if payload.longitude is not None:
        obj.longitude = payload.longitude

    # always set last_updated to ingestion time
    obj.last_updated = ingestion_ts
    if payload.source:
        obj.source = payload.source

    db.commit()
    db.refresh(obj)
    return obj, geometry_flagged, flag_reason


def farms_within_radius(db: Session, lat: float, lon: float, radius_km: float):
    # naive scan (SQLite); for larger data, add RTree/SpatiaLite later
    q = db.query(models.Farm).filter(models.Farm.latitude.isnot(None), models.Farm.longitude.isnot(None))
    results = []
    for f in q:
        d = haversine_km(lat, lon, f.latitude, f.longitude)
        if d <= radius_km:
            results.append((f, d))
    return sorted(results, key=lambda x: x[1])
