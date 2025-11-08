from __future__ import annotations

from typing import Optional, Union
import csv
import io
import json
import math
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy.orm import Session

from app import crud, models, schemas

Number = Union[int, float]


def is_valid_number(v: Optional[float]) -> bool:
    """Check if value is a real number (not None/NaN)."""
    try:
        return v is not None and not math.isnan(float(v))
    except Exception:
        return False


def round4(v: Optional[Number]) -> Optional[float]:
    """Round a value to 4 decimal places; return None if invalid."""
    try:
        return None if v is None or v == "" else round(float(v), 4)
    except (TypeError, ValueError):
        return None


def to_aware_utc(v: Optional[Union[str, datetime]]) -> datetime:
    """Convert input to an aware UTC datetime."""
    if v is None:
        return datetime.now(timezone.utc)
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc) if v.tzinfo else v.replace(tzinfo=timezone.utc)
    ts = pd.to_datetime(v, errors="coerce", utc=True)
    if pd.isna(ts):
        return datetime.now(timezone.utc)
    return ts.to_pydatetime()


def flatten_coords(coords):
    """Flatten nested GeoJSON coordinates to a list of [lon, lat] pairs."""
    if not isinstance(coords, (list, tuple)):
        return []
    if len(coords) == 2 and all(isinstance(v, (int, float)) for v in coords):
        return [coords]
    out = []
    for c in coords:
        out.extend(flatten_coords(c))
    return out


def representative_point_from_geometry(geom: Optional[dict]) -> Optional[tuple[float, float]]:
    """Get (lat, lon) from GeoJSON: point itself or centroid of vertices."""
    if not geom or not isinstance(geom, dict):
        return None
    gtype = geom.get("type")
    coords = geom.get("coordinates")

    if gtype == "Point" and isinstance(coords, (list, tuple)) and len(coords) >= 2:
        lon, lat = coords[0], coords[1]
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return (lat, lon)

    pts = flatten_coords(coords)
    if not pts:
        return None
    lats = lons = n = 0
    for p in pts:
        if isinstance(p, (list, tuple)) and len(p) >= 2:
            lon, lat = p[0], p[1]
            if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
                lats += lat
                lons += lon
                n += 1
    if n == 0:
        return None
    return (lats / n, lons / n)


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in kilometers."""
    R = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))


def farm_rep_point(f: models.Farm, use: str = "auto") -> Optional[tuple[float, float]]:
    """Choose a farm's (lat, lon) using 'latlon', 'geometry', or 'auto'."""
    use = (use or "auto").lower()

    if use == "latlon":
        if is_valid_number(getattr(f, "latitude", None)) and is_valid_number(getattr(f, "longitude", None)):
            return float(f.latitude), float(f.longitude)
        return None

    if use == "geometry":
        return representative_point_from_geometry(getattr(f, "geometry", None))

    if is_valid_number(getattr(f, "latitude", None)) and is_valid_number(getattr(f, "longitude", None)):
        return float(f.latitude), float(f.longitude)
    return representative_point_from_geometry(getattr(f, "geometry", None))


def process_csv_content(content: str, db: Session) -> dict:
    """Parse CSV text and upsert farms; derive lat/lon from geometry."""
    reader = csv.DictReader(io.StringIO(content))
    count = 0
    flags: list[dict] = []
    ingestion_ts = datetime.now(timezone.utc)

    for row in reader:
        geom = json.loads(row["geometry"]) if row.get("geometry") else None

        # Derive (lat, lon) from geometry, same approach as GeoJSON ingest
        lat = lon = None
        if geom:
            rep = representative_point_from_geometry(geom)  # (lat, lon) for Point or centroid for others
            if rep:
                lat, lon = rep
                lat, lon = round4(lat), round4(lon)

        acreage_val = row.get("acreage")
        acreage = None
        if acreage_val not in (None, ""):
            try:
                acreage = float(acreage_val)
            except (TypeError, ValueError):
                acreage = None

        payload = schemas.FarmBase(
            farm_id=str(row["farm_id"]),
            farm_name=row.get("farm_name") or None,
            acreage=acreage,
            latitude=lat,
            longitude=lon,
            geometry=geom,
            source="csv",
            last_updated=to_aware_utc(row.get("last_updated")),
        )

        _, gflag, reason = crud.upsert_farm(db, payload, ingestion_ts=ingestion_ts)
        if gflag:
            flags.append({"farm_id": payload.farm_id, "reason": reason})
        count += 1

    return {"ingested": count, "geometry_flags": flags}


def process_geojson_payload(geojson: dict, db: Session) -> dict:
    """Parse GeoJSON (Feature/FeatureCollection) and upsert farms."""
    gtype = geojson.get("type")
    if gtype == "FeatureCollection":
        features = geojson.get("features", []) or []
    elif gtype == "Feature":
        features = [geojson]
    else:
        raise ValueError("Body must be GeoJSON Feature or FeatureCollection")

    count = 0
    flags: list[dict] = []
    ingestion_ts = datetime.now(timezone.utc)

    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")

        lat = lon = None
        if geom:
            rep = crud.representative_point_from_geometry(geom)
            if rep:
                lat, lon = rep
                lat, lon = round4(lat), round4(lon)

        if "farm_id" not in props:
            raise ValueError("Feature properties must include 'farm_id'")

        acreage_val = props.get("acreage")
        acreage = None
        if acreage_val not in (None, ""):
            try:
                acreage = float(acreage_val)
            except (TypeError, ValueError):
                acreage = None

        payload = schemas.FarmBase(
            farm_id=str(props["farm_id"]),
            farm_name=props.get("farm_name") or None,
            acreage=acreage,
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
