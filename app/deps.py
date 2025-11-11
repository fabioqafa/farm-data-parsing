from app.ingest_service import FarmIngestService

def get_ingest_service() -> FarmIngestService:
    return FarmIngestService()