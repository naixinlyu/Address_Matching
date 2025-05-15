"""
End‑to‑end pipeline:
  1 parse.py     – fill street_* columns
  2 match.py     – exact + fuzzy
  3 fallback.py  – soundex layer
  4 export CSVs  – matched / unmatched
  5 write metrics.json (runtime, peak RSS, cost)
Usage:
    docker compose run app run_pipeline.py --workers 6
"""

import argparse, subprocess, sys, time, pathlib, psycopg2, os, json, psutil

STAGES   = ("parse.py", "match.py", "fallback.py")
OUT_DIR  = pathlib.Path("/output"); OUT_DIR.mkdir(exist_ok=True)

def rss_mb():
    """Current resident set size in MB."""
    return psutil.Process().memory_info().rss / 1_048_576

parser = argparse.ArgumentParser()
parser.add_argument("--workers", type=int, default=4, help="future parallelism knob")
args = parser.parse_args()

# run the three stages
metrics   = {"rss_start_mb": round(rss_mb(), 1)}
start_all = time.time()
peak_rss  = metrics["rss_start_mb"]

for stage in STAGES:
    t0 = time.time()
    if subprocess.run([sys.executable, stage]).returncode:
        sys.exit(f"{stage} failed")
    metrics[f"{stage}_sec"] = round(time.time() - t0, 1)
    peak_rss = max(peak_rss, rss_mb())

metrics["total_sec"]   = round(time.time() - start_all, 1)
metrics["peak_rss_mb"] = round(peak_rss, 1)

print(f"Pipeline finished in {metrics['total_sec']/60:,.2f} minutes")
print(f"Peak RSS: {peak_rss:,.1f} MB")

# export matched / unmatched CSVs
conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    user=os.getenv("PGUSER", "addrmatch"),
    password=os.getenv("PGPASSWORD", "pwd"),
    dbname=os.getenv("PGDATABASE", "addrdb"),
)
cur = conn.cursor()

with (OUT_DIR / "matched.csv").open("w", encoding="utf8") as f:
    cur.copy_expert("""
        COPY (
          SELECT id, address_id, confidence, match_type
          FROM transactions
          WHERE address_id IS NOT NULL
        ) TO STDOUT WITH CSV HEADER
    """, f)
print("Wrote /output/matched.csv")

with (OUT_DIR / "unmatched.csv").open("w", encoding="utf8") as f:
    cur.copy_expert("""
        COPY (
          SELECT id,
                 address_line_1 AS raw_address,
                 COALESCE(failure_reason,'failed parsed') AS reason
          FROM transactions
          WHERE address_id IS NULL
        ) TO STDOUT WITH CSV HEADER
    """, f)
print("Wrote /output/unmatched.csv")

cur.close()
conn.close()

# write metrics.json
(OUT_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))
print("Metrics → /output/metrics.json")