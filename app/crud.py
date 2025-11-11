from datetime import datetime, timezone
from typing import Optional, Tuple
from sqlalchemy.orm import Session
from app import models
from app.utils import (
    to_aware_utc,
    representative_point_from_geometry,
    haversine_km,
    round4,
)

# ---------- tiny, single-purpose helpers ----------

def _aware(ts: Optional[datetime]) -> datetime:
    return to_aware_utc(ts)

def _is_newer(existing_ts: datetime, incoming_ts: datetime) -> bool:
    return incoming_ts >= existing_ts

def _rep_point(geom: Optional[dict]) -> Optional[tuple[float, float]]:
    return representative_point_from_geometry(geom) if geom else None

def _derive_latlon_from_geom(geom: Optional[dict]) -> tuple[Optional[float], Optional[float]]:
    rp = _rep_point(geom)
    if not rp:
        return (None, None)
    lat = round4(float(rp[0]))
    lon = round4(float(rp[1]))
    return lat, lon

def _rounded_latlon(lat: Optional[float], lon: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    return round4(lat), round4(lon)

def _insert_new_farm(
    db: Session,
    payload,
    ingestion_ts: datetime,
) -> models.Farm:
    # Derive lat/lon from geometry if present; else use payload’s explicit lat/lon
    lat, lon = _derive_latlon_from_geom(payload.geometry)
    if lat is None and lon is None:
        lat, lon = payload.latitude, payload.longitude

    lat, lon = _rounded_latlon(lat, lon)
    source_ts = _aware(payload.last_updated) if payload.last_updated is not None else None

    obj = models.Farm(
        farm_id=payload.farm_id,
        farm_name=payload.farm_name,
        acreage=payload.acreage,
        latitude=lat,
        longitude=lon,
        geometry=payload.geometry,
        source=payload.source,
        last_updated=source_ts if source_ts is not None else ingestion_ts,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def _update_scalars_if_newer(
    obj: models.Farm,
    payload,
    existing_ts: datetime,
) -> None:
    incoming_ts = _aware(payload.last_updated)
    if payload.farm_name and _is_newer(existing_ts, incoming_ts):
        obj.farm_name = payload.farm_name
    if payload.acreage is not None and _is_newer(existing_ts, incoming_ts):
        obj.acreage = payload.acreage

def _decide_geometry_merge(
    old_geom: Optional[dict],
    new_geom: Optional[dict],
    geom_diff_threshold_km: float,
    *,
    existing_ts: datetime,
    incoming_ts: Optional[datetime],
) -> Tuple[bool, bool, Optional[str]]:
    """
    Returns (accept_update, geometry_flagged, reason)
    """
    if not new_geom:
        return False, False, None

    if incoming_ts is not None and incoming_ts < existing_ts:
        return False, False, None 

    old_pt = _rep_point(old_geom) if old_geom else None
    new_pt = _rep_point(new_geom)

    if old_pt and new_pt:
        d_km = haversine_km(old_pt[0], old_pt[1], new_pt[0], new_pt[1])
        if d_km > geom_diff_threshold_km:
            return False, True, f"Geometry shift {d_km:.2f} km > {geom_diff_threshold_km} km"
        return True, False, None

    # If we can’t compute, accept (since it's not older)
    return True, False, None

def _apply_geometry_and_sync_latlon(
    obj: models.Farm,
    payload,
    accept_geom_update: bool,
    geometry_flagged: bool,
    *,
    existing_ts: datetime,
    incoming_ts: Optional[datetime],
) -> None:
    """
    - If flagged: keep existing geometry/lat/lon unchanged.
    - If accepted:
        - set obj.geometry = payload.geometry
        - set lat/lon derived from obj.geometry (representative point)
    - If not accepted and not flagged:
        - geometry stays; but if geometry is None, allow explicit lat/lon from payload
    """
    if geometry_flagged:
        return 

    if accept_geom_update:
        obj.geometry = payload.geometry
        lat, lon = _derive_latlon_from_geom(obj.geometry)
        if lat is not None and lon is not None:
            obj.latitude = lat
            obj.longitude = lon
        return

    # No new geometry was applied
    if obj.geometry:
        # keep lat/lon derived from current geometry
        lat, lon = _derive_latlon_from_geom(obj.geometry)
        if lat is not None and lon is not None:
            obj.latitude = lat
            obj.longitude = lon
    else:
        # No geometry at all: only accept explicit lat/lon if not stale
        if incoming_ts is None or incoming_ts >= existing_ts:
            if payload.latitude is not None:
                obj.latitude = round4(payload.latitude)
            if payload.longitude is not None:
                obj.longitude = round4(payload.longitude)


def _finalize_metadata(obj: models.Farm, *, source: Optional[str], source_last_updated: Optional[datetime]) -> None:
    if source_last_updated is not None:
        existing_ts = _aware(obj.last_updated)
        if source_last_updated >= existing_ts:
            obj.last_updated = source_last_updated

    if source:
        obj.source = source

# ---------- thin orchestrator ----------

def upsert_farm(
    db: Session,
    payload,
    *,
    ingestion_ts: Optional[datetime] = None,
    geom_diff_threshold_km: float = 5.0,
) -> tuple[models.Farm, bool, Optional[str]]:
    """
    Orchestrates insert/update using small, single-purpose helpers.
    Returns: (obj, geometry_flagged, flag_reason)
    """
    ingestion_ts = _aware(ingestion_ts)
    obj = db.get(models.Farm, payload.farm_id)

    if not obj:
        obj = _insert_new_farm(db, payload, ingestion_ts) #If we want now -> ingestion_ts=datetime.now(timezone.utc)
        return obj, False, None

    geometry_flagged = False
    flag_reason = None

    # 1) Selective scalar updates by recency/presence
    existing_ts = _aware(obj.last_updated)
    incoming_ts = _aware(payload.last_updated) if payload.last_updated is not None else None

    _update_scalars_if_newer(obj, payload, existing_ts)

    # 2) Geometry decision
    accept_geom_update, geometry_flagged, flag_reason = _decide_geometry_merge(
        obj.geometry,
        payload.geometry,
        geom_diff_threshold_km,
        existing_ts=existing_ts,
        incoming_ts=incoming_ts,
    )

    # 3) Geometry + lat/lon sync
    _apply_geometry_and_sync_latlon(
        obj,
        payload,
        accept_geom_update,
        geometry_flagged,
        existing_ts=existing_ts,
        incoming_ts=incoming_ts,
    )

    # 4) Timestamps/source
    _finalize_metadata(
            obj,
            source=payload.source,
            source_last_updated=_aware(payload.last_updated) if payload.last_updated is not None else None,
        )
    db.commit()
    db.refresh(obj)
    return obj, geometry_flagged, flag_reason


def farms_within_radius(db: Session, lat: float, lon: float, radius_km: float):
    # Pull all farms and decide usable coordinates per-row
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

# Old code for upsert farm
# def upsert_farm(
#     db,
#     payload,
#     *,
#     ingestion_ts: Optional[datetime] = None,
#     geom_diff_threshold_km: float = 5.0
# ) -> tuple[models.Farm, bool, Optional[str]]:
#     ingestion_ts = to_aware_utc(ingestion_ts)
#     obj = db.get(models.Farm, payload.farm_id)

#     # ---------- INSERT ----------
#     if not obj:
#         # derive lat/lon from geometry if available
#         lat, lon = payload.latitude, payload.longitude
#         if payload.geometry:
#             rep = representative_point_from_geometry(payload.geometry)
#             if rep:
#                 lat, lon = float(rep[0]), float(rep[1])

#         obj = models.Farm(
#             farm_id=payload.farm_id,
#             farm_name=payload.farm_name,
#             acreage=payload.acreage,
#             latitude=lat,
#             longitude=lon,
#             geometry=payload.geometry,
#             source=payload.source,
#             last_updated=ingestion_ts,
#         )
#         db.add(obj)
#         db.commit()
#         db.refresh(obj)
#         return obj, False, None

#     # ---------- UPDATE ----------
#     geometry_flagged = False
#     flag_reason = None
#     existing_ts = to_aware_utc(obj.last_updated)
#     incoming_ts = to_aware_utc(payload.last_updated)

#     # update farm_name / acreage only if newer and non-empty
#     if payload.farm_name and incoming_ts >= existing_ts:
#         obj.farm_name = payload.farm_name
#     if payload.acreage is not None and incoming_ts >= existing_ts:
#         obj.acreage = payload.acreage

#     # geometry merge + decision
#     if payload.geometry:
#         new_pt = representative_point_from_geometry(payload.geometry)
#         old_pt = representative_point_from_geometry(obj.geometry) if obj.geometry else None

#         if old_pt and new_pt:
#             d_km = haversine_km(old_pt[0], old_pt[1], new_pt[0], new_pt[1])
#             if d_km > geom_diff_threshold_km:
#                 geometry_flagged = True
#                 flag_reason = f"Geometry shift {d_km:.2f} km > {geom_diff_threshold_km} km"
#             else:
#                 obj.geometry = payload.geometry
#         else:
#             # accept incoming if previous not usable
#             obj.geometry = payload.geometry

#     # latitude/longitude handling
#     if not geometry_flagged:
#         # if we have geometry (either existing or just updated), derive lat/lon from it
#         if obj.geometry:
#             rep = representative_point_from_geometry(obj.geometry)
#             if rep:
#                 obj.latitude = float(rep[0])
#                 obj.longitude = float(rep[1])
#         else:
#             # no geometry: allow explicit lat/lon from payload
#             if payload.latitude is not None:
#                 obj.latitude = payload.latitude
#             if payload.longitude is not None:
#                 obj.longitude = payload.longitude
#     else:
#         # big shift flagged: keep existing lat/lon in sync with existing geometry (no change)
#         pass

#     # always set last_updated to ingestion time and update source if provided
#     obj.last_updated = ingestion_ts
#     if payload.source:
#         obj.source = payload.source

#     db.commit()
#     db.refresh(obj)
#     return obj, geometry_flagged, flag_reason
