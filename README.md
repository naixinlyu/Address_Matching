# Address Matching Pipeline

## Stack
- Python 3.11
- PostgreSQL 16
- Docker Compose

## Core Libraries
- `usaddress`
- `rapidfuzz`
- `phonetics`
- `psycopg2`
- `tqdm`

---

## 1. Repository Layout

| Path | Purpose |
|:-----|:--------|
| `schema.sql` | DDL (tables, indexes, extensions) |
| `ingest.py` | **Robust bulk loader**<br>• `COPY` → session TEMP / staging tables<br>• Casts with `NULLIF` → numeric cols never break |
| `parse.py` | **Normalize & tokenize** transactions addresses (`usaddress` + regex) |
| `match.py` | Exact + fuzzy matching (RapidFuzz WRatio ≥ 80) |
| `fallback.py` | Soundex waterfall for remaining misses |
| `run_pipeline.py` | End-to-end driver (+ writes CSV & metrics.json) |
| `simulate.py` | Stream-duplicate a small CSV up to **200M** rows in ≤1 GB RAM |
| `fastapi_app.py` | Lightweight **REST** wrapper (`/match_address`) |
| `Dockerfile` / `docker-compose.yml` | Fully reproducible runtime |

---

## 2. Quick Start (Docker)

```bash
# 0. Clone / unzip repo, cd into it
docker compose build        # One-time image build

# 1. Spin up Postgres
docker compose up -d db

# 2. (Optional) Generate a 200M-row CSV in a streamed fashion
docker compose run --rm app        python simulate.py          --src /data/transactions_2_11211.csv          --target 200000000

# 3. Ingest both files  
## 3.1 Run Schema.sql
Get-Content schema.sql -Raw | docker exec -i address_matching-1-db psql -U addrmatch -d addrdb

## 3.2 Ingest original CSV
docker compose run app ingest.py --transactions /data/transactions_2_11211.csv --addresses "/data/11211 Addresses.csv"

## 3.3 (Optional) Ingest 200M-row CSV
docker compose run --rm app ingest.py --transactions /code/transactions_upsampled.csv --addresses "/data/11211 Addresses.csv"

# 4. Run the full pipeline (parse → match → fallback → exports)
docker compose run --rm --entrypoint python app run_pipeline.py --workers 6
# Output:
# • /output/matched.csv (id, address_id, confidence, match_type)
# • /output/unmatched.csv (id, raw_address, reason)
# • /output/metrics.json (runtime & peak RSS)

# 5. Launch interactive API (Swagger UI on :8000/docs)
docker compose run --rm -p 8000:8000 app -m uvicorn fastapi_app:app --reload --host 0.0.0.0
```

You can paste this JSON into Swagger UI (`localhost:8000/docs`) to test:

```json
{
  "raw_address": "123 Withers St Apt 2A, Brooklyn, NY 11211"
}
```

---

## 3. Performance

- **Scales to ≥ 200 million rows** using `simulate.py` (stream duplication with < 1 GB RAM).
- **Ingestion** leverages PostgreSQL `COPY` into staging tables for maximum throughput.
- **Matching pipeline:**
  1. **Blocking** on *ZIP + street number* to avoid N² explosion.
  2. **Exact match** on normalized fields.
  3. **Fuzzy match** using RapidFuzz WRatio ≥ 80.
  4. **Phonetic fallback** with Soundex for residual records.
- **Metrics automatically written** to `/output/metrics.json`, including:
  - Total runtime (seconds)
  - Peak memory (RSS, MB)

---

## 4. Design Notes

| Aspect | Approach |
|:-------|:---------|
| **Trade-offs** | Prioritize speed & scalability over perfect address normalization. |
| **Libraries / APIs** | `usaddress`, `rapidfuzz`, `phonetics`, `psycopg2`, `tqdm` |
| **Blocking strategy** | Candidate pool trimmed to same **ZIP + house number** |
| **Fallback waterfall** | **Exact → Fuzzy (RapidFuzz) → Soundex** |
| **Assumptions** | All addresses lie in NYC ZIP 11211.<br>No paid validation APIs used (hook available for USPS/SmartyStreets if desired). |
