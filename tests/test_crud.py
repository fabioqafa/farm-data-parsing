# tests/test_crud.py
from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app import models, schemas, crud
from app.db import Base


def same_moment(a: datetime, b: datetime) -> bool:
    """Compare datetimes ignoring tz-awareness differences."""
    if a.tzinfo is None:
        a = a.replace(tzinfo=timezone.utc)
    if b.tzinfo is None:
        b = b.replace(tzinfo=timezone.utc)
    return a == b


def mk_payload(
    *,
    farm_id: str = "F001",
    farm_name: str | None = "Alpha",
    acreage: float | None = 100.0,
    latitude: float | None = 41.33,
    longitude: float | None = 19.82,
    geometry: dict | None = None,
    source: str = "csv",
    last_updated: datetime | str = datetime(2025, 11, 1, 10, 0, tzinfo=timezone.utc),
) -> schemas.FarmBase:
    return schemas.FarmBase(
        farm_id=farm_id,
        farm_name=farm_name,
        acreage=acreage,
        latitude=latitude,
        longitude=longitude,
        geometry=geometry,
        source=source,
        last_updated=last_updated,
    )


@pytest.fixture(scope="function")
def db_session():
    """Fresh in-memory SQLite session per test."""
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


def test_upsert_inserts_new_farm(db_session):
    """Insert: creates record and sets last_updated to ingestion_ts."""
    ingestion_ts = datetime(2025, 11, 6, 12, 0, tzinfo=timezone.utc)
    payload = mk_payload()
    obj, flagged, reason = crud.upsert_farm(db_session, payload, ingestion_ts=ingestion_ts)

    assert isinstance(obj, models.Farm)
    assert obj.farm_id == "F001"
    assert obj.farm_name == "Alpha"
    assert obj.acreage == 100.0
    assert obj.latitude == 41.33
    assert obj.longitude == 19.82
    assert obj.source == "csv"
    assert same_moment(obj.last_updated, ingestion_ts)
    assert flagged is False
    assert reason is None


def test_upsert_updates_name_and_acreage_when_newer_and_present(db_session):
    """Update: newer payload with values updates farm_name and acreage."""
    first_ingest = datetime(2025, 11, 1, 9, 0, tzinfo=timezone.utc)
    crud.upsert_farm(db_session, mk_payload(farm_name="Old", acreage=50.0), ingestion_ts=first_ingest)

    newer_payload = mk_payload(
        farm_name="NewName",
        acreage=75.0,
        last_updated=datetime(2025, 11, 2, 10, 0, tzinfo=timezone.utc),
    )
    second_ingest = datetime(2025, 11, 2, 12, 0, tzinfo=timezone.utc)
    obj, flagged, _ = crud.upsert_farm(db_session, newer_payload, ingestion_ts=second_ingest)

    assert obj.farm_name == "NewName"
    assert obj.acreage == 75.0
    assert same_moment(obj.last_updated, second_ingest)
    assert flagged is False


def test_upsert_does_not_update_when_older_or_empty_values(db_session):
    """Update: older or empty incoming values do not overwrite existing."""
    first_ingest = datetime(2025, 11, 3, 9, 0, tzinfo=timezone.utc)
    crud.upsert_farm(db_session, mk_payload(farm_name="KeepMe", acreage=120.0), ingestion_ts=first_ingest)

    older_payload = mk_payload(
        farm_name="",
        acreage=None,
        last_updated=datetime(2025, 11, 2, 10, 0, tzinfo=timezone.utc),
    )
    second_ingest = datetime(2025, 11, 4, 12, 0, tzinfo=timezone.utc)
    obj, _, _ = crud.upsert_farm(db_session, older_payload, ingestion_ts=second_ingest)

    assert obj.farm_name == "KeepMe"
    assert obj.acreage == 120.0
    assert same_moment(obj.last_updated, second_ingest)


def test_upsert_geometry_small_shift_updates_geometry_and_latlon(db_session):
    """Geometry: small shift (≤ threshold) updates geometry and lat/lon."""
    orig_geom = {"type": "Point", "coordinates": [19.8170, 41.3290]}
    first_ingest = datetime(2025, 11, 5, 9, 0, tzinfo=timezone.utc)
    crud.upsert_farm(
        db_session,
        mk_payload(geometry=orig_geom, latitude=None, longitude=None),
        ingestion_ts=first_ingest,
    )

    new_geom_close = {"type": "Point", "coordinates": [19.8200, 41.3300]}
    newer_payload = mk_payload(
        geometry=new_geom_close,
        latitude=None,
        longitude=None,
        last_updated=datetime(2025, 11, 5, 10, 0, tzinfo=timezone.utc),
    )
    second_ingest = datetime(2025, 11, 5, 12, 0, tzinfo=timezone.utc)

    obj, flagged, reason = crud.upsert_farm(db_session, newer_payload, ingestion_ts=second_ingest)

    assert flagged is False
    assert reason is None
    assert obj.geometry == new_geom_close
    lon, lat = new_geom_close["coordinates"]
    assert pytest.approx(obj.latitude, rel=1e-6) == float(lat)
    assert pytest.approx(obj.longitude, rel=1e-6) == float(lon)
    assert same_moment(obj.last_updated, second_ingest)


def test_upsert_geometry_large_shift_flags_and_keeps_old_geometry(db_session):
    """Geometry: large shift (> threshold) flags and keeps old geometry/latlon (lat/lon derived from original geometry)."""
    orig_geom = {"type": "Point", "coordinates": [19.8170, 41.3290]}
    first_ingest = datetime(2025, 11, 5, 9, 0, tzinfo=timezone.utc)
    crud.upsert_farm(
        db_session,
        mk_payload(geometry=orig_geom, latitude=None, longitude=None),
        ingestion_ts=first_ingest,
    )

    new_geom_far = {"type": "Point", "coordinates": [19.70, 41.3290]}
    newer_payload = mk_payload(
        geometry=new_geom_far,
        latitude=None,
        longitude=None,
        last_updated=datetime(2025, 11, 5, 10, 0, tzinfo=timezone.utc),
    )
    second_ingest = datetime(2025, 11, 5, 12, 0, tzinfo=timezone.utc)

    obj, flagged, reason = crud.upsert_farm(db_session, newer_payload, ingestion_ts=second_ingest)

    assert flagged is True
    assert reason is not None and "Geometry shift" in reason
    assert obj.geometry == orig_geom
    # With the new logic, lat/lon remain derived from the original geometry
    assert pytest.approx(obj.latitude, rel=1e-9) == 41.3290
    assert pytest.approx(obj.longitude, rel=1e-9) == 19.8170
    assert same_moment(obj.last_updated, second_ingest)


def test_upsert_accepts_direct_lat_lon_when_no_geometry(db_session):
    """Insert: accepts direct lat/lon when geometry is absent."""
    first_ingest = datetime(2025, 11, 6, 9, 0, tzinfo=timezone.utc)
    payload = mk_payload(geometry=None, latitude=41.4, longitude=19.9)
    obj, flagged, _ = crud.upsert_farm(db_session, payload, ingestion_ts=first_ingest)

    assert flagged is False
    assert obj.latitude == 41.4
    assert obj.longitude == 19.9
    assert obj.geometry is None


def test_upsert_direct_latlon_overrides_even_if_geometry_present(db_session):
    """Update: when geometry is present, lat/lon are derived from geometry (explicit lat/lon are ignored)."""
    first_ingest = datetime(2025, 11, 6, 9, 0, tzinfo=timezone.utc)
    crud.upsert_farm(db_session, mk_payload(geometry=None), ingestion_ts=first_ingest)

    newer_payload = mk_payload(
        geometry={"type": "Point", "coordinates": [19.8, 41.3]},
        latitude=50.0,
        longitude=10.0,
        last_updated=datetime(2025, 11, 6, 10, 0, tzinfo=timezone.utc),
    )
    second_ingest = datetime(2025, 11, 6, 12, 0, tzinfo=timezone.utc)
    obj, _, _ = crud.upsert_farm(db_session, newer_payload, ingestion_ts=second_ingest)

    # With the updated logic, geometry drives lat/lon when present
    assert pytest.approx(obj.latitude, rel=1e-9) == 41.3
    assert pytest.approx(obj.longitude, rel=1e-9) == 19.8


def test_farms_within_radius_uses_latlon_and_sorts(db_session):
    """Radius: uses stored lat/lon and returns nearest-first."""
    ingestion = datetime(2025, 11, 6, 8, 0, tzinfo=timezone.utc)
    base_lat, base_lon = 41.329, 19.817

    crud.upsert_farm(
        db_session, mk_payload(farm_id="A", latitude=base_lat, longitude=base_lon), ingestion_ts=ingestion
    )
    crud.upsert_farm(
        db_session, mk_payload(farm_id="B", latitude=base_lat + 0.05, longitude=base_lon), ingestion_ts=ingestion
    )
    crud.upsert_farm(
        db_session, mk_payload(farm_id="C", latitude=base_lat + 0.20, longitude=base_lon), ingestion_ts=ingestion
    )

    res = crud.farms_within_radius(db_session, base_lat, base_lon, 50.0, use="latlon")
    ids_in_order = [f.farm_id for f, _ in res]
    assert ids_in_order[:3] == ["A", "B", "C"]


def test_farms_within_radius_uses_geometry_when_no_latlon(db_session):
    """Radius: supports geometry-only farms via representative point."""
    ingestion = datetime(2025, 11, 6, 8, 0, tzinfo=timezone.utc)

    center = {"type": "Point", "coordinates": [19.817, 41.329]}
    far = {"type": "Point", "coordinates": [19.0, 41.329]}

    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="G1", latitude=None, longitude=None, geometry=center),
        ingestion_ts=ingestion,
    )
    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="G2", latitude=None, longitude=None, geometry=far),
        ingestion_ts=ingestion,
    )

    res = crud.farms_within_radius(db_session, 41.329, 19.817, 50.0, use="geometry")
    ids = [f.farm_id for f, _ in res]
    assert "G1" in ids
    assert "G2" not in ids


def test_farms_within_radius_auto_prefers_latlon_then_geometry(db_session):
    """Radius: 'auto' prefers lat/lon but falls back to geometry."""
    ingestion = datetime(2025, 11, 6, 8, 0, tzinfo=timezone.utc)

    geom = {"type": "Point", "coordinates": [19.80, 41.33]}
    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="X", latitude=41.331, longitude=19.801, geometry=geom),
        ingestion_ts=ingestion,
    )

    geom2 = {"type": "Point", "coordinates": [19.60, 41.33]}
    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="Y", latitude=None, longitude=None, geometry=geom2),
        ingestion_ts=ingestion,
    )

    res = crud.farms_within_radius(db_session, 41.329, 19.817, 25.0, use="auto")
    ids = [f.farm_id for f, _ in res]
    assert "X" in ids
    assert "Y" in ids


def test_last_updated_always_equals_ingestion_ts(db_session):
    """Timestamps: stored last_updated always equals ingestion_ts."""
    incoming_last_updated = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ingestion_ts = datetime(2025, 11, 6, 9, 0, tzinfo=timezone.utc)

    obj, _, _ = crud.upsert_farm(
        db_session,
        mk_payload(last_updated=incoming_last_updated),
        ingestion_ts=ingestion_ts,
    )
    assert same_moment(obj.last_updated, ingestion_ts)

    obj2, _, _ = crud.upsert_farm(
        db_session,
        mk_payload(farm_name="Later", last_updated=incoming_last_updated + timedelta(days=10)),
        ingestion_ts=ingestion_ts + timedelta(hours=2),
    )
    assert same_moment(obj2.last_updated, ingestion_ts + timedelta(hours=2))
