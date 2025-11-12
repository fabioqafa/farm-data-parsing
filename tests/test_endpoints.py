from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Callable

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from app import models
from app.db import Base, get_db
from app.deps import get_ingest_service
from app.ingest_service import FarmIngestService
from app.main import app


UTC = timezone.utc


class ClockStub:
    """Mutable clock so tests can control ingestion timestamps."""

    def __init__(self, initial: datetime | None = None):
        self._now = initial or datetime(2025, 1, 1, tzinfo=UTC)

    def set(self, value: datetime) -> None:
        self._now = value

    def advance(self, **delta_kwargs) -> None:
        self._now += timedelta(**delta_kwargs)

    def __call__(self) -> datetime:
        return self._now


@pytest.fixture
def api_client():
    """FastAPI TestClient wired to an isolated in-memory SQLite DB."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)

    def override_get_db():
        db = TestingSessionLocal()
        try:
            yield db
        finally:
            db.close()

    clock = ClockStub()
    ingest_service = FarmIngestService(clock=clock)

    def override_ingest_service():
        return ingest_service

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_ingest_service] = override_ingest_service

    with TestClient(app) as client:
        yield client, TestingSessionLocal, clock

    app.dependency_overrides.clear()


def get_farm(sessionmaker_factory: Callable[[], Session], farm_id: str) -> models.Farm | None:
    with sessionmaker_factory() as session:
        return session.get(models.Farm, farm_id)


def query_all_farms(sessionmaker_factory: Callable[[], Session]) -> list[models.Farm]:
    with sessionmaker_factory() as session:
        return session.query(models.Farm).order_by(models.Farm.farm_id).all()


def csv_geom(value: dict) -> str:
    """Embed GeoJSON inside a CSV field with doubled quotes."""
    return json.dumps(value).replace('"', '""')


def test_get_farms_returns_empty_list_when_db_clean(api_client):
    client, _, _ = api_client

    resp = client.get("/farms")

    assert resp.status_code == 200, resp.json()
    assert resp.json() == []


def test_ingest_csv_creates_records_and_lists_them(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 11, 6, 10, 0, tzinfo=UTC))
    geom_cf1 = csv_geom({"type": "Point", "coordinates": [19.8192, 41.3278]})
    csv_content = (
        "farm_id,farm_name,acreage,geometry,last_updated\n"
        f'CF1,Alpha,145.5,"{geom_cf1}",2025-11-01T00:00:00Z\n'
        "CF2,,34.2,,\n"
    )

    resp = client.post(
        "/ingest/csv",
        files={"file": ("farms.csv", csv_content.encode("utf-8"), "text/csv")},
    )

    assert resp.status_code == 200, resp.json()
    assert resp.json() == {"ingested": 2, "geometry_flags": []}
    farms = query_all_farms(sessionmaker_factory)
    assert [f.farm_id for f in farms] == ["CF1", "CF2"]
    cf1 = get_farm(sessionmaker_factory, "CF1")
    assert cf1.farm_name == "Alpha"
    assert cf1.acreage == 145.5
    assert pytest.approx(cf1.latitude, rel=1e-6) == 41.3278
    assert pytest.approx(cf1.longitude, rel=1e-6) == 19.8192
    cf2 = get_farm(sessionmaker_factory, "CF2")
    assert cf2.farm_name is None
    assert cf2.geometry is None


def test_ingest_csv_rejects_invalid_utf8(api_client):
    client, _, _ = api_client
    bad_bytes = b"\x80\x81\x82"

    resp = client.post(
        "/ingest/csv",
        files={"file": ("bad.csv", bad_bytes, "text/csv")},
    )

    assert resp.status_code == 400
    assert "UTF-8" in resp.json()["detail"]


def test_ingest_csv_invalid_geometry_returns_422(api_client):
    client, _, _ = api_client
    csv_content = "farm_id,farm_name,acreage,geometry,last_updated\n" 'CF3,Beta,10,"{not-json}",2025-01-01T00:00:00Z\n'

    resp = client.post(
        "/ingest/csv",
        files={"file": ("farms.csv", csv_content.encode("utf-8"), "text/csv")},
    )

    assert resp.status_code == 422


def test_ingest_csv_respects_update_rules_and_geometry_threshold(api_client):
    client, sessionmaker_factory, clock = api_client
    base_time = datetime(2025, 11, 1, 12, 0, tzinfo=UTC)
    clock.set(base_time)
    base_geom = csv_geom({"type": "Point", "coordinates": [19.82, 41.33]})
    base_csv = (
        "farm_id,farm_name,acreage,geometry,last_updated\n"
        f'CF4,Original,100.0,"{base_geom}",2025-11-01T12:00:00Z\n'
    )
    resp = client.post(
        "/ingest/csv",
        files={"file": ("base.csv", base_csv.encode("utf-8"), "text/csv")},
    )
    assert resp.status_code == 200, resp.json()

    # Older data with empty values should not override
    clock.advance(days=1)
    older_csv = (
        "farm_id,farm_name,acreage,geometry,last_updated\n"
        'CF4,,,"",2025-10-01T00:00:00Z\n'
    )
    resp = client.post(
        "/ingest/csv",
        files={"file": ("older.csv", older_csv.encode("utf-8"), "text/csv")},
    )
    assert resp.status_code == 200, resp.json()
    farm = get_farm(sessionmaker_factory, "CF4")
    assert farm.farm_name == "Original"
    assert farm.acreage == 100.0
    original_geom = farm.geometry

    # Newer data with close-by geometry updates both scalars and geometry
    clock.advance(days=1)
    newer_geom = csv_geom({"type": "Point", "coordinates": [19.8205, 41.3302]})
    newer_csv = (
        "farm_id,farm_name,acreage,geometry,last_updated\n"
        f'CF4,Updated,133.5,"{newer_geom}",2025-11-05T00:00:00Z\n'
    )
    resp = client.post(
        "/ingest/csv",
        files={"file": ("newer.csv", newer_csv.encode("utf-8"), "text/csv")},
    )
    assert resp.status_code == 200, resp.json()
    assert resp.json()["geometry_flags"] == []

    farm = get_farm(sessionmaker_factory, "CF4")
    assert farm.farm_name == "Updated"
    assert farm.acreage == 133.5
    assert farm.geometry != original_geom
    assert pytest.approx(farm.latitude, rel=1e-6) == 41.3302
    assert pytest.approx(farm.longitude, rel=1e-6) == 19.8205


def test_get_farm_returns_404_when_missing(api_client):
    client, _, _ = api_client

    resp = client.get("/farms/DOES-NOT-EXIST")

    assert resp.status_code == 404
    assert resp.json()["detail"] == "Farm not found"


def test_get_farm_returns_persisted_record(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 9, 1, 9, 0, tzinfo=UTC))
    geom_cf5 = csv_geom({"type": "Point", "coordinates": [19.8, 41.32]})
    csv_content = (
        "farm_id,farm_name,acreage,geometry,last_updated\n"
        f'CF5,Barn,75.0,"{geom_cf5}",2025-09-01T09:00:00Z\n'
    )
    resp = client.post(
        "/ingest/csv",
        files={"file": ("barn.csv", csv_content.encode("utf-8"), "text/csv")},
    )
    assert resp.status_code == 200, resp.json()

    resp = client.get("/farms/CF5")

    assert resp.status_code == 200, resp.json()
    body = resp.json()
    assert body["farm_id"] == "CF5"
    assert body["farm_name"] == "Barn"
    assert body["acreage"] == 75.0


def test_ingest_geojson_accepts_feature_collection(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 8, 1, tzinfo=UTC))
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "farm_id": "GF1",
                    "farm_name": "Geo Farm",
                    "acreage": 50.0,
                    "last_updated": "2025-08-01T00:00:00Z",
                },
                "geometry": {"type": "Point", "coordinates": [19.81, 41.33]},
            },
            {
                "type": "Feature",
                "properties": {
                    "farm_id": "GF2",
                    "farm_name": "Centroid",
                    "acreage": 80.0,
                    "last_updated": "2025-08-02T00:00:00Z",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[[19.8, 41.32], [19.82, 41.32], [19.82, 41.35], [19.8, 41.35], [19.8, 41.32]]],
                },
            },
        ],
    }

    resp = client.post("/ingest/geojson", json=payload)

    assert resp.status_code == 200
    assert resp.json() == {"ingested": 2, "geometry_flags": []}
    assert get_farm(sessionmaker_factory, "GF1") is not None
    assert get_farm(sessionmaker_factory, "GF2") is not None


def test_ingest_geojson_requires_farm_id(api_client):
    client, _, _ = api_client
    payload = {
        "type": "Feature",
        "properties": {"farm_name": "No Id"},
        "geometry": {"type": "Point", "coordinates": [19.8, 41.32]},
    }

    resp = client.post("/ingest/geojson", json=payload)

    assert resp.status_code == 422
    assert "farm_id" in resp.json()["detail"]


def test_ingest_geojson_geometry_shift_gets_flagged_and_preserves_original(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 7, 1, tzinfo=UTC))
    base_feature = {
        "type": "Feature",
        "properties": {
            "farm_id": "GF3",
            "farm_name": "Stable",
            "acreage": 60,
            "last_updated": "2025-07-01T00:00:00Z",
        },
        "geometry": {"type": "Point", "coordinates": [19.8, 41.32]},
    }
    resp = client.post("/ingest/geojson", json=base_feature)
    assert resp.status_code == 200, resp.json()

    clock.advance(days=1)
    far_feature = {
        "type": "Feature",
        "properties": {
            "farm_id": "GF3",
            "farm_name": "Moved Stable",
            "acreage": 70,
            "last_updated": "2025-07-05T00:00:00Z",
        },
        "geometry": {"type": "Point", "coordinates": [20.8, 42.32]},
    }

    resp = client.post("/ingest/geojson", json=far_feature)

    assert resp.status_code == 200
    flags = resp.json()["geometry_flags"]
    assert len(flags) == 1
    assert flags[0]["farm_id"] == "GF3"
    assert "Geometry shift" in flags[0]["reason"]
    farm = get_farm(sessionmaker_factory, "GF3")
    assert farm.farm_name == "Moved Stable"  # newer scalar values accepted
    assert farm.geometry["coordinates"] == [19.8, 41.32]


def test_ingest_geojson_small_shift_updates_geometry(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 6, 1, tzinfo=UTC))
    feature = {
        "type": "Feature",
        "properties": {
            "farm_id": "GF4",
            "farm_name": "MinorMove",
            "acreage": 90,
            "last_updated": "2025-06-01T00:00:00Z",
        },
        "geometry": {"type": "Point", "coordinates": [19.80, 41.30]},
    }
    client.post("/ingest/geojson", json=feature)

    clock.advance(days=1)
    moved = {
        "type": "Feature",
        "properties": {
            "farm_id": "GF4",
            "farm_name": "MinorMove",
            "acreage": 90,
            "last_updated": "2025-06-02T00:00:00Z",
        },
        "geometry": {"type": "Point", "coordinates": [19.801, 41.301]},
    }
    resp = client.post("/ingest/geojson", json=moved)

    assert resp.status_code == 200
    assert resp.json()["geometry_flags"] == []
    farm = get_farm(sessionmaker_factory, "GF4")
    assert pytest.approx(farm.latitude, rel=1e-6) == 41.301
    assert pytest.approx(farm.longitude, rel=1e-6) == 19.801


def test_ingest_xml_creates_records_and_uses_point_coords(api_client):
    client, sessionmaker_factory, clock = api_client
    clock.set(datetime(2025, 5, 1, tzinfo=UTC))
    xml_body = """
    <FeatureCollection>
      <Feature>
        <Properties>
          <farm_id>XF1</farm_id>
          <farm_name>XML Farm</farm_name>
          <acreage>55</acreage>
          <last_updated>2025-05-01T00:00:00Z</last_updated>
        </Properties>
        <Geometry>
          <type>Point</type>
          <coordinates>
            <lon>19.77</lon>
            <lat>41.31</lat>
          </coordinates>
        </Geometry>
      </Feature>
    </FeatureCollection>
    """.strip()

    resp = client.post(
        "/ingest/xml",
        files={"file": ("farms.xml", xml_body.encode("utf-8"), "application/xml")},
    )

    assert resp.status_code == 200
    assert resp.json() == {"ingested": 1, "geometry_flags": []}
    farm = get_farm(sessionmaker_factory, "XF1")
    assert pytest.approx(farm.latitude, rel=1e-6) == 41.31
    assert pytest.approx(farm.longitude, rel=1e-6) == 19.77


def test_ingest_xml_invalid_payload_returns_422(api_client):
    client, _, _ = api_client
    bad_xml = "<Feature><Properties><farm_name>Missing closing tag"

    resp = client.post(
        "/ingest/xml",
        files={"file": ("bad.xml", bad_xml.encode("utf-8"), "application/xml")},
    )

    assert resp.status_code == 422


def test_get_farms_within_filters_and_sorts_by_distance(api_client):
    client, _, clock = api_client
    clock.set(datetime(2025, 4, 1, tzinfo=UTC))
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "farm_id": "WF1",
                    "farm_name": "Near",
                    "acreage": 10,
                    "last_updated": "2025-04-01T00:00:00Z",
                },
                "geometry": {"type": "Point", "coordinates": [19.81, 41.33]},
            },
            {
                "type": "Feature",
                "properties": {
                    "farm_id": "WF2",
                    "farm_name": "Far",
                    "acreage": 20,
                    "last_updated": "2025-04-01T00:00:00Z",
                },
                "geometry": {"type": "Point", "coordinates": [20.81, 42.33]},
            },
        ],
    }
    client.post("/ingest/geojson", json=payload)

    resp = client.get("/farms/within", params={"lat": 41.33, "lon": 19.81, "radius": 20})

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["farm"]["farm_id"] == "WF1"
    assert body[0]["distance_km"] >= 0


def test_get_farms_lists_current_state(api_client):
    client, _, clock = api_client
    clock.set(datetime(2025, 3, 1, tzinfo=UTC))
    payload = {
        "type": "Feature",
        "properties": {
            "farm_id": "LF1",
            "farm_name": "List Farm",
            "acreage": 88,
            "last_updated": "2025-03-01T00:00:00Z",
        },
        "geometry": {"type": "Point", "coordinates": [19.7, 41.3]},
    }
    client.post("/ingest/geojson", json=payload)

    resp = client.get("/farms")

    assert resp.status_code == 200
    farms = resp.json()
    assert farms and farms[0]["farm_id"] == "LF1"
