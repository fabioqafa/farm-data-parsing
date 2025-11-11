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
    """Insert: creates record; last_updated comes from SOURCE timestamp."""
    payload = mk_payload()
    obj, flagged, reason = crud.upsert_farm(db_session, payload)

    assert isinstance(obj, models.Farm)
    assert obj.farm_id == "F001"
    assert obj.farm_name == "Alpha"
    assert obj.acreage == 100.0
    assert obj.latitude == 41.33
    assert obj.longitude == 19.82
    assert obj.source == "csv"
    assert same_moment(obj.last_updated, payload.last_updated)  # from source
    assert flagged is False
    assert reason is None


def test_upsert_updates_name_and_acreage_when_newer_and_present(db_session):
    """Update: newer payload with values updates farm_name/acreage; last_updated advances to newer SOURCE ts."""
    crud.upsert_farm(db_session, mk_payload(farm_name="Old", acreage=50.0))

    newer_payload = mk_payload(
        farm_name="NewName",
        acreage=75.0,
        last_updated=datetime(2025, 11, 2, 10, 0, tzinfo=timezone.utc),
    )
    obj, flagged, _ = crud.upsert_farm(db_session, newer_payload)

    assert obj.farm_name == "NewName"
    assert obj.acreage == 75.0
    assert same_moment(obj.last_updated, newer_payload.last_updated)
    assert flagged is False


def test_upsert_does_not_update_when_older_or_empty_values(db_session):
    """Update: older SOURCE ts + empty values do not overwrite; last_updated does not regress."""
    # Insert original with SOURCE ts 2025-11-01
    insert_payload = mk_payload(farm_name="KeepMe", acreage=120.0,
                                last_updated=datetime(2025, 11, 1, 10, 0, tzinfo=timezone.utc))
    obj0, _, _ = crud.upsert_farm(db_session, insert_payload)

    # Incoming is older (2025-10-01) and empty values
    older_payload = mk_payload(
        farm_name="",
        acreage=None,
        last_updated=datetime(2025, 10, 1, 10, 0, tzinfo=timezone.utc),
    )
    obj, _, _ = crud.upsert_farm(db_session, older_payload)

    assert obj.farm_name == "KeepMe"
    assert obj.acreage == 120.0
    assert same_moment(obj.last_updated, obj0.last_updated)  # did not regress


def test_upsert_geometry_small_shift_updates_geometry_and_latlon(db_session):
    """Geometry: small shift (≤ threshold) updates geometry & lat/lon; last_updated = SOURCE ts."""
    orig_geom = {"type": "Point", "coordinates": [19.8170, 41.3290]}
    crud.upsert_farm(
        db_session,
        mk_payload(geometry=orig_geom, latitude=None, longitude=None),
    )

    new_geom_close = {"type": "Point", "coordinates": [19.8200, 41.3300]}
    newer_payload = mk_payload(
        geometry=new_geom_close,
        latitude=None,
        longitude=None,
        last_updated=datetime(2025, 11, 5, 10, 0, tzinfo=timezone.utc),
    )

    obj, flagged, reason = crud.upsert_farm(db_session, newer_payload)

    assert flagged is False
    assert reason is None
    assert obj.geometry == new_geom_close
    lon, lat = new_geom_close["coordinates"]
    assert pytest.approx(obj.latitude, rel=1e-6) == float(lat)
    assert pytest.approx(obj.longitude, rel=1e-6) == float(lon)
    assert same_moment(obj.last_updated, newer_payload.last_updated)


def test_upsert_geometry_large_shift_flags_and_keeps_old_geometry(db_session):
    """Geometry: large shift (> threshold) flags; geometry unchanged; lat/lon from original; last_updated = newer SOURCE ts."""
    orig_geom = {"type": "Point", "coordinates": [19.8170, 41.3290]}
    crud.upsert_farm(
        db_session,
        mk_payload(geometry=orig_geom, latitude=None, longitude=None),
    )

    new_geom_far = {"type": "Point", "coordinates": [19.70, 41.3290]}
    newer_payload = mk_payload(
        geometry=new_geom_far,
        latitude=None,
        longitude=None,
        last_updated=datetime(2025, 11, 5, 10, 0, tzinfo=timezone.utc),
    )

    obj, flagged, reason = crud.upsert_farm(db_session, newer_payload)

    assert flagged is True
    assert reason is not None and "Geometry shift" in reason
    assert obj.geometry == orig_geom
    assert pytest.approx(obj.latitude, rel=1e-9) == 41.3290
    assert pytest.approx(obj.longitude, rel=1e-9) == 19.8170
    assert same_moment(obj.last_updated, newer_payload.last_updated)


def test_upsert_accepts_direct_lat_lon_when_no_geometry(db_session):
    """Insert: accepts direct lat/lon when geometry is absent; last_updated = SOURCE ts."""
    payload = mk_payload(geometry=None, latitude=41.4, longitude=19.9)
    obj, flagged, _ = crud.upsert_farm(db_session, payload)

    assert flagged is False
    assert obj.latitude == 41.4
    assert obj.longitude == 19.9
    assert obj.geometry is None
    assert same_moment(obj.last_updated, payload.last_updated)


def test_upsert_rounds_lat_lon_to_four_decimals(db_session):
    """Lat/Lon: values are rounded to 4 decimal places regardless of source."""
    payload = mk_payload(geometry=None, latitude=41.3333333, longitude=19.8777777)
    obj, _, _ = crud.upsert_farm(db_session, payload)

    assert pytest.approx(obj.latitude, rel=1e-9) == 41.3333
    assert pytest.approx(obj.longitude, rel=1e-9) == 19.8778

    geom_payload = mk_payload(
        geometry={"type": "Point", "coordinates": [19.87654321, 41.12345678]},
        latitude=None,
        longitude=None,
        last_updated=datetime(2025, 11, 10, 10, 0, tzinfo=timezone.utc),
    )
    obj2, _, _ = crud.upsert_farm(db_session, geom_payload)

    assert pytest.approx(obj2.latitude, rel=1e-9) == 41.1235
    assert pytest.approx(obj2.longitude, rel=1e-9) == 19.8765


def test_upsert_direct_latlon_overrides_even_if_geometry_present(db_session):
    """Update: when geometry present, lat/lon derived from geometry; explicit lat/lon ignored; last_updated = SOURCE ts."""
    crud.upsert_farm(db_session, mk_payload(geometry=None))

    newer_payload = mk_payload(
        geometry={"type": "Point", "coordinates": [19.8, 41.3]},
        latitude=50.0,
        longitude=10.0,
        last_updated=datetime(2025, 11, 6, 10, 0, tzinfo=timezone.utc),
    )
    obj, _, _ = crud.upsert_farm(db_session, newer_payload)

    assert pytest.approx(obj.latitude, rel=1e-9) == 41.3
    assert pytest.approx(obj.longitude, rel=1e-9) == 19.8
    assert same_moment(obj.last_updated, newer_payload.last_updated)


def test_farms_within_radius(db_session):
    """Radius: supports geometry-only farms via representative point."""
    center = {"type": "Point", "coordinates": [19.817, 41.329]}
    far = {"type": "Point", "coordinates": [19.0, 41.329]}

    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="G1", latitude=None, longitude=None, geometry=center),
    )
    crud.upsert_farm(
        db_session,
        mk_payload(farm_id="G2", latitude=None, longitude=None, geometry=far),
    )

    res = crud.farms_within_radius(db_session, 41.329, 19.817, 50.0)
    ids = [f.farm_id for f, _ in res]
    assert "G1" in ids
    assert "G2" not in ids


def test_last_updated_source_monotonic_and_nonregressing(db_session):
    """Timestamps: last_updated reflects SOURCE time, only advances, never regresses."""
    # 1) Insert with older source time
    source_old = datetime(2024, 1, 1, tzinfo=timezone.utc)
    obj, _, _ = crud.upsert_farm(db_session, mk_payload(last_updated=source_old))
    assert same_moment(obj.last_updated, source_old)

    # 2) Update with even older source -> should NOT change last_updated
    source_older = datetime(2023, 1, 1, tzinfo=timezone.utc)
    obj2, _, _ = crud.upsert_farm(
        db_session,
        mk_payload(farm_name="NoChange", last_updated=source_older),
    )
    assert same_moment(obj2.last_updated, source_old)

    # 3) Update with newer source -> last_updated advances
    source_newer = source_old + timedelta(days=10)
    obj3, _, _ = crud.upsert_farm(
        db_session,
        mk_payload(farm_name="Advance", last_updated=source_newer),
    )
    assert same_moment(obj3.last_updated, source_newer)
