# Farms API (SQLite) — Geospatial Farm Data Interoperability

FastAPI service that ingests farm data from CSV and GeoJSON, unifies records in SQLite via SQLAlchemy, applies update/merge rules (including geometry-change checks), and exposes endpoints to retrieve farms and query farms within a radius.

This implementation is designed to satisfy the “Coding challenge: Geospatial farm data interoperability service” (see Challenge_2ndRound_BackEnd.pdf at the repo root). A summary of the challenge requirements and how this project meets them is included below.


## Quick Start

- Python 3.10+
- Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install fastapi uvicorn sqlalchemy pandas pydantic pytest
```

- Run the API:

```bash
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

- Open docs at http://127.0.0.1:8000/docs

Note: A helper `start_app.sh` is provided; it’s most suitable when running in a PowerShell-enabled environment. Otherwise, use the `uvicorn` command above.


## Data Model (SQLite via SQLAlchemy)

Each farm record includes:

- `farm_id` (str, primary key)
- `farm_name` (optional str)
- `acreage` (optional float)
- `latitude` / `longitude` (optional floats; convenience point)
- `geometry` (JSON; e.g., GeoJSON `Point`, `Polygon`, etc.)
- `source` ("csv" | "geojson")
- `last_updated` (timezone-aware UTC datetime)


## Ingestion and Update/Merge Rules

- Create if `farm_id` does not exist.
- For existing farms:
  - `farm_name` / `acreage`: update only when the incoming record has a newer `last_updated` and a non-empty value.
  - `geometry`: compute representative point for both existing and incoming geometry; if the shift is > 5 km, flag the update (do not overwrite geometry). Otherwise, accept the new geometry; if it’s a `Point`, update `latitude`/`longitude` accordingly.
  - Direct `latitude`/`longitude` in CSV are accepted when provided.
  - `last_updated` of the stored record is always set to the ingestion timestamp.

Representative point logic:
- `Point`: use the point as-is (GeoJSON order `[lon, lat]` is handled to map into `(lat, lon)`).
- `Polygon` / `Multi*`: compute a simple centroid approximation by averaging vertices.


## API Endpoints

- `GET /farms` — list all farms.
- `GET /farms/{farm_id}` — retrieve a single farm.
- `GET /farms/within?lat={lat}&lon={lon}&radius={km}&use={auto|latlon|geometry}` — list farms within `radius` kilometers of the given coordinate.
  - `use` selector:
    - `auto` (default): prefer stored `latitude/longitude`; if missing, fall back to geometry representative point.
    - `latlon`: use only `latitude/longitude` fields.
    - `geometry`: use only geometry-derived representative point.
- `POST /ingest/csv` — multipart upload with form field `file` containing the CSV.
- `POST /ingest/geojson` — body is a GeoJSON `Feature` or `FeatureCollection`.


## CSV Format

Expected columns: `farm_id, farm_name, acreage, latitude, longitude, geometry, last_updated`

- `geometry` (if present) must be a JSON string representing a valid GeoJSON geometry.
- Empty strings for optional values are treated as missing.

Example row:

```csv
farm_id,farm_name,acreage,latitude,longitude,geometry,last_updated
F001,Sample Farm,21,41.3278,19.8192,"{\"type\":\"Point\",\"coordinates\":[19.8192,41.3278]}",2025-11-06T22:38:28Z
```


## GeoJSON Format

Accepts a `Feature` or `FeatureCollection`. Each feature must include `properties.farm_id`. Optional properties include `farm_name`, `acreage`, and `last_updated` (ISO 8601). Geometry may be `Point`, `Polygon`, etc.

Minimal example:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F100",
        "farm_name": "Example",
        "acreage": 50.0,
        "last_updated": "2025-01-01T00:00:00Z"
      },
      "geometry": { "type": "Point", "coordinates": [19.8170, 41.3290] }
    }
  ]
}
```


## Usage Examples

Ingest CSV:

```bash
curl -X POST "http://127.0.0.1:8000/ingest/csv" \
  -F "file=@farms_cases_21.csv;type=text/csv"
```

Ingest GeoJSON:

```bash
curl -X POST "http://127.0.0.1:8000/ingest/geojson" \
  -H "Content-Type: application/json" \
  -d @example.geojson
```

Within-radius query (10 km around 41.4, 19.9):

```bash
curl "http://127.0.0.1:8000/farms/within?lat=41.4&lon=19.9&radius=10"
```

Force geometry-based distances:

```bash
curl "http://127.0.0.1:8000/farms/within?lat=41.4&lon=19.9&radius=10&use=geometry"
```


## Tested Data

This code was tested with:

- CSV file: `farms_cases_21.csv` (included at the repo root).
- GeoJSON payloads listed below (applied in order to test updates, newer timestamps, geometry moves > 5 km, and polygon handling):

Provided payloads:

```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F200",
        "farm_name": "Initial Farm",
        "acreage": 100.0,
        "last_updated": "2025-01-01T00:00:00Z"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [19.8170, 41.3290]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F200",
        "farm_name": "Renamed Farm",
        "acreage": 120.0,
        "last_updated": "2030-01-01T00:00:00Z"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [19.8200, 41.3300]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F200",
        "farm_name": "Renamed Farm Big Move",
        "acreage": 130.0,
        "last_updated": "2031-01-01T00:00:00Z"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [19.0000, 41.3290]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F200",
        "farm_name": "",
        "acreage": "",
        "last_updated": "2000-01-01T00:00:00Z"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [19.8210, 41.3305]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F201",
        "farm_name": "Second Farm",
        "acreage": 75.0,
        "last_updated": "2025-02-01T12:00:00Z"
      },
      "geometry": {
        "type": "Point",
        "coordinates": [19.8010, 41.3310]
      }
    },
    {
      "type": "Feature",
      "properties": {
        "farm_id": "F201",
        "farm_name": "Second Farm Updated",
        "acreage": 80.0,
        "last_updated": "2030-06-01T00:00:00Z"
      },
      "geometry": {
        "type": "Polygon",
        "coordinates": [
          [
            [19.80, 41.33],
            [19.82, 41.33],
            [19.82, 41.35],
            [19.80, 41.35],
            [19.80, 41.33]
          ]
        ]
      }
    }
  ]
}

```

Additional 14 payloads used for coverage (points, polygons, later/older timestamps, small/big moves):

```json
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F202",
      "farm_name": "Poly Farm",
      "acreage": 90.0,
      "last_updated": "2026-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Polygon",
      "coordinates": [[[19.81, 41.33], [19.83, 41.33], [19.83, 41.34], [19.81, 41.34], [19.81, 41.33]]]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F203",
      "farm_name": "Point Near",
      "acreage": 55.0,
      "last_updated": "2027-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.822, 41.331]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F203",
      "farm_name": "Point Near Updated",
      "acreage": 58.0,
      "last_updated": "2029-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.823, 41.332]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F204",
      "farm_name": "Big Shift",
      "acreage": 60.0,
      "last_updated": "2030-03-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.6, 41.2]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F205",
      "farm_name": "Older Update",
      "acreage": 70.0,
      "last_updated": "2010-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.82, 41.33]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F206",
      "farm_name": "Newer Update",
      "acreage": 72.0,
      "last_updated": "2032-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.821, 41.331]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F207",
      "farm_name": "Multi Vertex",
      "acreage": 85.0,
      "last_updated": "2028-05-01T00:00:00Z"
    },
    "geometry": {
      "type": "Polygon",
      "coordinates": [[[19.79, 41.32], [19.84, 41.32], [19.84, 41.36], [19.79, 41.36], [19.79, 41.32]]]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F208",
      "farm_name": "Edge Case Empty Name",
      "acreage": 95.0,
      "last_updated": "2029-07-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.825, 41.333]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F209",
      "farm_name": "Centroid Test",
      "acreage": 110.0,
      "last_updated": "2031-01-01T00:00:00Z"
    },
    "geometry": {
      "type": "Polygon",
      "coordinates": [[[19.8, 41.34], [19.82, 41.34], [19.82, 41.37], [19.8, 41.37], [19.8, 41.34]]]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F210",
      "farm_name": "Minor Move",
      "acreage": 66.0,
      "last_updated": "2030-09-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.8205, 41.3302]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F210",
      "farm_name": "Minor Move Updated",
      "acreage": 66.0,
      "last_updated": "2031-09-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.8208, 41.3304]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F211",
      "farm_name": "Far Away",
      "acreage": 150.0,
      "last_updated": "2030-11-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [20.5, 41.9]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F212",
      "farm_name": "Ring",
      "acreage": 120.0,
      "last_updated": "2030-12-01T00:00:00Z"
    },
    "geometry": {
      "type": "Polygon",
      "coordinates": [[[19.79, 41.33], [19.83, 41.33], [19.83, 41.35], [19.79, 41.35], [19.79, 41.33]]]
    }
  },
  {
    "type": "Feature",
    "properties": {
      "farm_id": "F213",
      "farm_name": "Another Point",
      "acreage": 45.0,
      "last_updated": "2028-02-01T00:00:00Z"
    },
    "geometry": {
      "type": "Point",
      "coordinates": [19.815, 41.328]
    }
  }

```


## Challenge Requirements (Summary)

From `Challenge_2ndRound_BackEnd.pdf` (included in the repo):

- Ingest and normalize data from two sources:
  - Source A (GeoJSON): features with `farm_id`, `farm_name`, `acreage`, `geometry`.
  - Source B (CSV upload): rows with `farm_id`, `farm_name`, `latitude`, `longitude`, `acreage`.
- Store unified records with: `farm_id`, `farm_name`, `geometry`, `acreage`, `source`, `last_updated`.
- Handle updates and geometry merging:
  - Create if new `farm_id`.
  - For existing: update `farm_name`/`acreage` if newer (`last_updated`) and provided.
  - Compare geometry; if shift > 5 km, flag rather than overwrite.
- Provide API endpoints:
  - Ingest CSV and GeoJSON
  - Retrieve farms (list and by `farm_id`)
  - Farms within radius (e.g., `GET /farms?lat=..&long=..&radius=50`) — implemented here as `GET /farms/within` with `lat`, `lon`, `radius` (and `use` selector).
- Evaluation focuses on structure, data modeling, geospatial reasoning, and extensibility.

How this project meets the requirements:

- Endpoints: `/ingest/csv`, `/ingest/geojson`, `/farms`, `/farms/{farm_id}`, `/farms/within`.
- Data model: SQLite via SQLAlchemy with fields per spec.
- Merge logic: respects `last_updated`, non-empty updates, and geometry shift threshold of 5 km.
- Geospatial functions: Haversine distance for both updates and radius search; representative point for geometries.
- Extensibility: clear layering (`app/utils.py`, `app/crud.py`, `app/models.py`, `app/schemas.py`, `app/main.py`).


## Testing

- Unit tests: `tests/test_crud.py`

Run tests:

```bash
pytest -q
```


## Notes and Tips

- GeoJSON coordinates are `[longitude, latitude]`; the API consistently interprets these to internal `(lat, lon)` for distance calculations.
- The `/farms/within` endpoint supports a `use` parameter allowing you to pick `latlon` vs. `geometry` or let the service choose `auto`.
- If a farm is just outside your radius, slightly increase `radius` to validate spatial proximity (e.g., 10.5 km vs. 10 km).
