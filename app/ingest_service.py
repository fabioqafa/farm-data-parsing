# app/ingest_service.py
from __future__ import annotations
from typing import Callable, Dict, Any
from datetime import datetime, timezone
from sqlalchemy.orm import Session

from app import crud, schemas
from app.utils import (
    representative_point_from_geometry,
    round4,
    to_aware_utc,
)

Clock = Callable[[], datetime]
Upsert = Callable[..., tuple]

class FarmIngestService:
    def __init__(self, *, clock: Clock | None = None, upsert: Upsert | None = None):
        # DI
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._upsert = upsert or crud.upsert_farm

    @staticmethod
    def _parse_acreage(value: Any) -> float | None:
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def ingest(self, source, db: Session) -> Dict[str, Any]:
        count = 0
        flags: list[dict] = []
        ingestion_ts = self._clock()

        for r in source.records():
            lat = lon = None
            if r.get("geometry"):
                rep = representative_point_from_geometry(r["geometry"])
                if rep:
                    lat, lon = round4(rep[0]), round4(rep[1])

            payload = schemas.FarmBase(
                farm_id=r["farm_id"],
                farm_name=r.get("farm_name"),
                acreage=self._parse_acreage(r.get("acreage")),
                latitude=lat,
                longitude=lon,
                geometry=r.get("geometry"),
                source=r["source"],
                last_updated=to_aware_utc(r.get("last_updated")),
            )

            _, gflag, reason = self._upsert(db, payload, ingestion_ts=ingestion_ts)
            if gflag:
                flags.append({"farm_id": payload.farm_id, "reason": reason})
            count += 1

        return {"ingested": count, "geometry_flags": flags}
