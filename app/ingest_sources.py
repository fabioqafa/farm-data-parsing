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

        def local_name(tag: str) -> str:
            return tag.split('}', 1)[-1] if '}' in tag else tag

        def find_child(parent: ET.Element, name: str) -> Optional[ET.Element]:
            name = name.lower()
            for ch in list(parent):
                if local_name(ch.tag).lower() == name:
                    return ch
            return None

        def text_of(parent: ET.Element, *names: str) -> Optional[str]:
            """Find first matching child by local name and return stripped text."""
            wanted = {n.lower() for n in names}
            for ch in list(parent):
                if local_name(ch.tag).lower() in wanted:
                    txt = (ch.text or "").strip()
                    if txt:
                        return txt
            return None

        def parse_coordinates(coords_el: ET.Element) -> Any:
            # Supports:
            #   <coordinates><lon>..</lon><lat>..</lat></coordinates>
            #   <coordinates>lon,lat</coordinates>
            #   <coordinates>lon lat</coordinates>
            lon_el = find_child(coords_el, "lon")
            lat_el = find_child(coords_el, "lat")
            if lon_el is not None and lat_el is not None:
                try:
                    return [float((lon_el.text or "").strip()), float((lat_el.text or "").strip())]
                except ValueError:
                    pass

            raw = (coords_el.text or "").strip()
            if raw:
                # try comma or space separated lon/lat
                parts = [p for p in raw.replace(",", " ").split() if p]
                if len(parts) == 2:
                    try:
                        return [float(parts[0]), float(parts[1])]
                    except ValueError:
                        return raw  # keep as-is if not numeric
                return raw  # keep as-is for non-Point/simple cases

            # Fallback: try first two numeric child texts
            nums: list[float] = []
            for ch in list(coords_el):
                try:
                    nums.append(float((ch.text or "").strip()))
                except Exception:
                    pass
            if len(nums) >= 2:
                return nums[:2]
            return None

        def parse_geometry(geom_el: Optional[ET.Element]) -> Any:
            if geom_el is None:
                return None
            gtype = text_of(geom_el, "type")
            coords_el = find_child(geom_el, "coordinates")
            coords = parse_coordinates(coords_el) if coords_el is not None else None

            # If we have type + coords for common cases, return a GeoJSON-like dict
            if gtype and coords is not None:
                return {"type": gtype, "coordinates": coords}

            # Otherwise, keep a minimal dict or raw string representation
            if gtype:
                return {"type": gtype}
            return ET.tostring(geom_el, encoding="unicode")

        def feature_elements(r: ET.Element) -> list[ET.Element]:
            lname = local_name(r.tag).lower()
            if lname == "feature":
                return [r]
            if lname == "featurecollection":
                return [ch for ch in list(r) if local_name(ch.tag).lower() == "feature"]
            # generic: collect any nested Feature elements
            return [el for el in r.iter() if local_name(el.tag).lower() == "feature"]

        feats = feature_elements(root)
        if not feats:
            raise ValueError("XML must contain a <Feature> or <FeatureCollection> with one or more <Feature> children")

        for feat in feats:
            props_el = find_child(feat, "properties")
            geom_el = find_child(feat, "geometry")

            # Properties can also be direct children if <properties> is missing
            def prop_value(*names: str) -> Optional[str]:
                if props_el is not None:
                    v = text_of(props_el, *names)
                    if v:
                        return v
                return text_of(feat, *names)

            farm_id = prop_value("farm_id", "id")
            if not farm_id:
                raise ValueError("Each <Feature> must include properties with 'farm_id' or 'id'")

            farm_name = prop_value("farm_name", "name", "title")
            acreage = prop_value("acreage", "area")
            last_updated = prop_value("last_updated", "updated", "lastUpdate", "last_updated_at")

            geometry = parse_geometry(geom_el)

            yield {
                "farm_id": str(farm_id),
                "farm_name": farm_name or None,
                "acreage": acreage,
                "geometry": geometry,
                "last_updated": last_updated,
                "source": "xml",
            }
