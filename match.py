# Exact + fuzzy join from transactions → addresses
import os
import psycopg2
from rapidfuzz import process, fuzz
from parse import parse_address
import re

THRESH = 80 

def clean_duplicate_units(address):
    # Remove duplicate unit information
    # Match patterns like "Unit X, Unit X" or "Unit X, Unit X,"
    pattern = r'(Unit\s+[^,]+),\s*\1'
    return re.sub(pattern, r'\1', address)

def match_batch(limit=10_000):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST","localhost"),
        user=os.getenv("PGUSER","addrmatch"),
        password=os.getenv("PGPASSWORD","pwd"),
        dbname=os.getenv("PGDATABASE","addrdb"),
    )
    cur_stream = conn.cursor(name="stream")
    cur_stream.itersize = limit
    cur_stream.execute("""
        SELECT id, address_line_1, address_line_2, city, state, zip_code
        FROM transactions
        WHERE address_id IS NULL
    """)

    while True:
        rows = cur_stream.fetchmany(limit)
        if not rows: break
        _process(rows, conn)

    conn.commit()
    conn.close()

def _process(rows, conn):
    cur = conn.cursor()
    for tid, l1, l2, city, state, zc in rows:
        raw = ", ".join(filter(None, [l1, l2, city, state, zc]))
        # Clean duplicate units before parsing
        raw = clean_duplicate_units(raw)
        p   = parse_address(raw)

        # exact (relaxed)
        cur.execute("""
            SELECT address_id
            FROM addresses
            WHERE zip   = %(zip)s
              AND house = %(street_number)s
              AND LOWER(street) = LOWER(%(street_name)s)
        """, p)
        hit = cur.fetchone()
        if hit:
            _apply(cur, tid, hit[0], "exact", 1.0);  continue

        # fuzzy on ZIP block
        cur.execute("""
            SELECT street::text, address_id
            FROM addresses
            WHERE zip = %s
        """, (p["zip"],))
        rows2 = cur.fetchall()

        streets, ids = [], []
        for s, aid in rows2:
            if s:
                streets.append(s.lower())
                ids.append(aid)

        q = (p["street_name"] or "").lower()
        if q and streets:
            best, score, idx = process.extractOne(q, streets, scorer=fuzz.WRatio)
            if score >= THRESH:
                _apply(cur, tid, ids[idx], "fuzzy", score/100)

def _apply(cur, tid, aid, mtype, conf):
    cur.execute("""
        UPDATE transactions
        SET address_id=%s, match_type=%s, confidence=%s
        WHERE id=%s
    """, (aid, mtype, conf, tid))

if __name__ == "__main__":
    match_batch()
