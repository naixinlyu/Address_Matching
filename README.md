# Address Matching Pipeline

<table>
<tr><th>Stack</th><td>Python 3.11 · PostgreSQL 16 · Docker Compose</td></tr>
<tr><th>Core libs</th><td><code>usaddress</code>, <code>rapidfuzz</code>, <code>phonetics</code>, <code>psycopg2</code>, <code>tqdm</code></td></tr>
</table>

---

## 1  Repository layout
| path | purpose |
|------|---------|
| `schema.sql` | DDL (tables, indexes, extensions) |
| `ingest.py`  | **robust bulk loader**<br>• `COPY` → session TEMP / staging tables<br>• casts with `NULLIF` → numeric cols never break |
| `parse.py`   | <span title="usaddress + regex">normalize & tokenize</span> `transactions` addresses |
| `match.py`   | Exact + fuzzy matching (RapidFuzz WRatio ≥ 80) |
| `fallback.py`| Soundex waterfall for remaining misses |
| `run_pipeline.py` | end‑to‑end driver (+ writes CSV & `metrics.json`) |
| `simulate.py`| stream‑duplicate a small CSV up to **200 M** rows in ≤1 GB RAM |
| `fastapi_app.py` | Lightweight **REST** wrapper (`/match_address`) |
| `Dockerfile` / `docker‑compose.yml` | fully reproducible runtime |

---

## 2  Quick start (Docker)

```bash
# 0. Clone / unzip repo, cd into it
docker compose build        # 1‑time image build

# 1. Spin up Postgres
docker compose up -d db

# 2. (Optional) generate a 200 M‑row CSV in a streamed fashion
docker compose run --rm app \
       python simulate.py \
         --src /data/transactions_2_11211.csv \
         --target 200000000

# 3. Ingest both files
##    3.1 Ingest original CSV
docker compose run app ingest.py --transactions /data/transactions_2_11211.csv --addresses "/data/11211 Addresses.csv"  

##    3.2 Ingest 200 M‑row CSV c
docker compose run --rm app ingest.py --transactions /code/transactions_upsampled.csv --addresses "/data/11211 Addresses.csv"


# 4. Run the full pipeline (parse → match → fallback → exports)
docker compose run --rm app python run_pipeline.py --workers 6
#   • /output/matched.csv      (id, address_id, confidence, match_type)
#   • /output/unmatched.csv    (id, raw_address, reason)
#   • /output/metrics.json     (runtime & peak RSS)

# 5. Launch interactive API (Swagger UI on :8000/docs)
docker compose run --rm -p 8000:8000 app -m uvicorn fastapi_app:app --reload --host 0.0.0.0

You can paste this JSON into Swagger UI (localhost:8000/docs) to test:
{
  "raw_address": "123 Withers St Apt 2A, Brooklyn, NY 11211"
}


