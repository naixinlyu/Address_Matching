from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from rapidfuzz import process, fuzz
import psycopg2, os
from parse import parse_address

app = FastAPI(title="Address Matcher API")

class AddressIn(BaseModel):
    raw_address: str

def db():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        user=os.getenv("PGUSER", "addrmatch"),
        password=os.getenv("PGPASSWORD", "pwd"),
        dbname=os.getenv("PGDATABASE", "addrdb"),
    )

THRESH = 80 

@app.post("/match_address")
def match_address(inp: AddressIn):
    p = parse_address(inp.raw_address)

    if not p.get("zip") or not p.get("street_number"):
        raise HTTPException(422, "Address parse failed")

    conn = db(); cur = conn.cursor()

    # case‑insensitive exact
    cur.execute(
        """
        SELECT address_id
        FROM addresses
        WHERE zip   = %(zip)s
          AND house = %(street_number)s
          AND LOWER(street) = LOWER(%(street_name)s)
        LIMIT 1
        """,
        p,
    )
    hit = cur.fetchone()
    if hit:
        conn.close()
        return {
            "address_id": hit[0],
            "match_type": "exact",
            "confidence": 1.0,
        }

    # fuzzy inside ZIP block
    cur.execute(
        """
        SELECT street::text, address_id
        FROM addresses
        WHERE zip = %s
        """,
        (p["zip"],),
    )
    rows = cur.fetchall()
    streets, ids = zip(*[(s.lower(), aid) for s, aid in rows if s])

    q = (p["street_name"] or "").lower()
    if q and streets:
        best, score, idx = process.extractOne(q, streets, scorer=fuzz.WRatio)
        if score >= THRESH:
            conn.close()
            return {
                "address_id": ids[idx],
                "match_type": "fuzzy",
                "confidence": round(score / 100.0, 3),
            }

    conn.close()
    raise HTTPException(404, "No match above threshold")

