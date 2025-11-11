# app/ingest_sources.py
from __future__ import annotations
from typing import Iterable, Protocol, Optional, Any, Dict
import csv, io, json
import xml.etree.ElementTree as ET

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

class XmlSource(IngestSource):
    def __init__(self, xml_content: str):
        self._xml = xml_content

    def records(self) -> Iterable[Dict[str, Any]]:
        root = ET.fromstring(self._xml)

        def ln(tag: str) -> str:
            return tag.split('}', 1)[-1] if '}' in tag else tag

        def child(parent: ET.Element, name: str) -> Optional[ET.Element]:
            n = name.lower()
            for ch in list(parent):
                if ln(ch.tag).lower() == n:
                    return ch
            return None

        def text(parent: ET.Element, *names: str) -> Optional[str]:
            wanted = {n.lower() for n in names}
            for ch in list(parent):
                if ln(ch.tag).lower() in wanted:
                    t = (ch.text or "").strip()
                    if t:
                        return t
            return None

        def r4(x: float) -> float:
            return round(float(x), 4)

        def parse_geometry(geom_el: Optional[ET.Element]) -> Any:
            if geom_el is None:
                return None

            # Case 1: entire <geometry> is a JSON blob (pass through like CSV)
            raw_geom_text = (geom_el.text or "").strip()
            if raw_geom_text and raw_geom_text.lstrip().startswith(("[", "{")):
                try:
                    return json.loads(raw_geom_text)
                except json.JSONDecodeError:
                    pass

            gtype = text(geom_el, "type")
            coords_el = child(geom_el, "coordinates")
            if gtype is None and coords_el is None:
                return None

            coords: Any = None
            if coords_el is not None:
                raw_coords = (coords_el.text or "").strip()

                # JSON-like coordinates inside <coordinates> (Polygon/Multi*, or Point)
                if raw_coords and raw_coords.lstrip().startswith(("[", "{")):
                    try:
                        coords = json.loads(raw_coords)
                    except json.JSONDecodeError:
                        coords = None

                # Point-friendly <lon>/<lat>
                if coords is None:
                    lon_el = child(coords_el, "lon")
                    lat_el = child(coords_el, "lat")
                    if lon_el is not None and lat_el is not None:
                        try:
                            lon_v = r4(float((lon_el.text or "").strip()))
                            lat_v = r4(float((lat_el.text or "").strip()))
                            coords = [lon_v, lat_v]
                        except ValueError:
                            coords = None

                # Point-friendly "lon,lat" or "lon lat"
                if coords is None and raw_coords:
                    parts = [p for p in raw_coords.replace(",", " ").split() if p]
                    if len(parts) == 2:
                        try:
                            coords = [r4(float(parts[0])), r4(float(parts[1]))]
                        except ValueError:
                            coords = None

            # If it's a Point with numeric coords, ensure rounding (safety net)
            if gtype and isinstance(coords, list) and gtype.lower() == "point":
                if len(coords) == 2 and all(isinstance(v, (int, float)) for v in coords):
                    coords = [r4(coords[0]), r4(coords[1])]

            if gtype and coords is not None:
                return {"type": gtype, "coordinates": coords}
            if gtype:
                return {"type": gtype}
            return None

        def features(root_el: ET.Element) -> list[ET.Element]:
            name = ln(root_el.tag).lower()
            if name == "feature":
                return [root_el]
            if name == "featurecollection":
                return [ch for ch in list(root_el) if ln(ch.tag).lower() == "feature"]
            return [el for el in root_el.iter() if ln(el.tag).lower() == "feature"]

        feats = features(root)
        if not feats:
            raise ValueError("XML must contain a <Feature> or <FeatureCollection> with one or more <Feature> children")

        for feat in feats:
            props = child(feat, "properties")

            def prop(*names: str) -> Optional[str]:
                if props is not None:
                    v = text(props, *names)
                    if v:
                        return v
                return text(feat, *names)

            farm_id = prop("farm_id", "id")
            if not farm_id:
                raise ValueError("Feature properties must include 'farm_id'")

            yield {
                "farm_id": str(farm_id),
                "farm_name": prop("farm_name", "name", "title") or None,
                "acreage": prop("acreage", "area"),
                "geometry": parse_geometry(child(feat, "geometry")),  # Point coords rounded to 4
                "last_updated": prop("last_updated", "updated", "lastUpdate", "last_updated_at"),
                "source": "xml",
            }
