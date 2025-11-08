# app/ingest_sources.py
from __future__ import annotations
from typing import Iterable, Protocol, Optional, Any, Dict
import csv, io, json

class IngestSource(Protocol):
    def records(self) -> Iterable[Dict[str, Any]]:
        """Yield normalized dicts with keys:
        farm_id, farm_name, acreage, geometry, last_updated, source
        """
        ...

class CsvSource(IngestSource):
    def __init__(self, content: str):
        self._content = content

    def records(self) -> Iterable[Dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(self._content))
        for row in reader:
            geom = json.loads(row["geometry"]) if row.get("geometry") else None
            yield {
                "farm_id": str(row["farm_id"]),
                "farm_name": row.get("farm_name") or None,
                "acreage": row.get("acreage"),
                "geometry": geom,
                "last_updated": row.get("last_updated"),
                "source": "csv",
            }

class GeoJSONSource(IngestSource):
    def __init__(self, geojson: dict):
        self._geojson = geojson

    def records(self) -> Iterable[Dict[str, Any]]:
        gtype = self._geojson.get("type")
        if gtype == "FeatureCollection":
            features = self._geojson.get("features", []) or []
        elif gtype == "Feature":
            features = [self._geojson]
        else:
            raise ValueError("Body must be GeoJSON Feature or FeatureCollection")

        for feat in features:
            props = feat.get("properties") or {}
            geom = feat.get("geometry")
            if "farm_id" not in props:
                raise ValueError("Feature properties must include 'farm_id'")
            yield {
                "farm_id": str(props["farm_id"]),
                "farm_name": props.get("farm_name") or None,
                "acreage": props.get("acreage"),
                "geometry": geom,
                "last_updated": props.get("last_updated"),
                "source": "geojson",
            }
