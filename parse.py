import os, time, re, psycopg2, usaddress
from tqdm import tqdm

ABBREV = {"st":"street","rd":"road","ave":"avenue","blvd":"boulevard"}

def _clean(tok: str) -> str:
    tok = re.sub(r"[^a-z0-9]", "", tok.lower())
    return ABBREV.get(tok, tok)

def parse_address(raw: str) -> dict:
    tagged, _ = usaddress.tag(raw)
    return {
        "street_number": tagged.get("AddressNumber"),
        "street_name":  " ".join(_clean(t) for t in tagged.get("StreetName","").split()),
        "street_suffix": _clean(tagged.get("StreetNamePostType","") or ""),
        "unit_number":   tagged.get("OccupancyIdentifier"),
        "city":          tagged.get("PlaceName"),
        "state":         tagged.get("StateName"),
        "zip":           tagged.get("ZipCode"),
    }

# ----------------------------------------------------------------------
def main(batch=10_000):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        user=os.getenv("PGUSER", "addrmatch"),
        password=os.getenv("PGPASSWORD", "pwd"),
        dbname=os.getenv("PGDATABASE", "addrdb"),
    )
    cur_stream = conn.cursor(name="pstream", withhold=True)
    cur_stream.itersize = batch
    cur_stream.execute("""
        SELECT id, address_line_1, address_line_2, city, state, zip_code
        FROM transactions
        WHERE COALESCE(NULLIF(street_name, ''), NULL) IS NULL 
    """)

    prog = tqdm(total=cur_stream.rowcount if cur_stream.rowcount != -1 else None,
                desc="parsing", unit="row")

    while True:
        rows = cur_stream.fetchmany(batch)
        if not rows:
            break
        _write_rows(rows, conn)   
        conn.commit()             
        prog.update(len(rows))

    prog.close()
    conn.close()

def _write_rows(rows, conn):
    cur = conn.cursor()
    for tid, line1, line2, city, state, zip_code in rows:
        raw = ", ".join(filter(None, [line1, line2, city, state, zip_code]))
        p   = parse_address(raw)
        cur.execute("""
            UPDATE transactions
            SET street_number = %(street_number)s,
                street_name   = %(street_name)s,
                street_suffix = %(street_suffix)s,
                unit_number   = %(unit_number)s,
                city          = %(city)s,
                state         = %(state)s,
                zip           = %(zip)s
            WHERE id = %(tid)s
        """, {**p, "tid": tid})

if __name__ == "__main__":
    t0 = time.time()
    main()
    print(f"Parse.py finished in {time.time()-t0:.1f}s")

