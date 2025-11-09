from datetime import datetime
from typing import Optional
from sqlalchemy.orm import Session
from app import models
from app.utils import to_aware_utc, representative_point_from_geometry, haversine_km

def upsert_farm(
    db,
    payload,
    *,
    ingestion_ts: Optional[datetime] = None,
    geom_diff_threshold_km: float = 5.0
) -> tuple[models.Farm, bool, Optional[str]]:
    ingestion_ts = to_aware_utc(ingestion_ts)
    obj = db.get(models.Farm, payload.farm_id)

    # ---------- INSERT ----------
    if not obj:
        # derive lat/lon from geometry if available
        lat, lon = payload.latitude, payload.longitude
        if payload.geometry:
            rep = representative_point_from_geometry(payload.geometry)
            if rep:
                lat, lon = float(rep[0]), float(rep[1])

        obj = models.Farm(
            farm_id=payload.farm_id,
            farm_name=payload.farm_name,
            acreage=payload.acreage,
            latitude=lat,
            longitude=lon,
            geometry=payload.geometry,
            source=payload.source,
            last_updated=ingestion_ts,
        )
        db.add(obj)
        db.commit()
        db.refresh(obj)
        return obj, False, None

    # ---------- UPDATE ----------
    geometry_flagged = False
    flag_reason = None
    existing_ts = to_aware_utc(obj.last_updated)
    incoming_ts = to_aware_utc(payload.last_updated)

    # update farm_name / acreage only if newer and non-empty
    if payload.farm_name and incoming_ts >= existing_ts:
        obj.farm_name = payload.farm_name
    if payload.acreage is not None and incoming_ts >= existing_ts:
        obj.acreage = payload.acreage

    # geometry merge + decision
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
        else:
            # accept incoming if previous not usable
            obj.geometry = payload.geometry

    # latitude/longitude handling
    if not geometry_flagged:
        # if we have geometry (either existing or just updated), derive lat/lon from it
        if obj.geometry:
            rep = representative_point_from_geometry(obj.geometry)
            if rep:
                obj.latitude = float(rep[0])
                obj.longitude = float(rep[1])
        else:
            # no geometry: allow explicit lat/lon from payload
            if payload.latitude is not None:
                obj.latitude = payload.latitude
            if payload.longitude is not None:
                obj.longitude = payload.longitude
    else:
        # big shift flagged: keep existing lat/lon in sync with existing geometry (no change)
        pass

    # always set last_updated to ingestion time and update source if provided
    obj.last_updated = ingestion_ts
    if payload.source:
        obj.source = payload.source

    db.commit()
    db.refresh(obj)
    return obj, geometry_flagged, flag_reason


def farms_within_radius(db: Session, lat: float, lon: float, radius_km: float):
    # Pull all farms and decide usable coordinates per-row based on 'use'
    farms = db.query(models.Farm).all()
    results: list[tuple[models.Farm, float]] = []
    for f in farms:
        rep = representative_point_from_geometry(f.geometry)
        if not rep:
            continue
        f_lat, f_lon = rep  # (lat, lon)
        d = haversine_km(float(lat), float(lon), float(f_lat), float(f_lon))
        if d <= float(radius_km):
            results.append((f, d))
    return sorted(results, key=lambda x: x[1])
