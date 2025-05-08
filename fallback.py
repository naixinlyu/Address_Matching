# Soundex fallback layer after exact + fuzzy.
# Works with the new schema (`id` primary key).
import psycopg2, phonetics, os

def soundex_match():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        user=os.getenv("PGUSER", "addrmatch"),
        password=os.getenv("PGPASSWORD", "pwd"),
        dbname=os.getenv("PGDATABASE", "addrdb"),
    )
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id, street_name, zip, street_number
        FROM transactions
        WHERE address_id IS NULL
          AND street_name IS NOT NULL
        """
    )

    for tid, sname, zip_code, num in cur.fetchall():
        sname = (sname or "").strip()
        if not sname.isalpha():
            continue
        sx = phonetics.soundex(sname)
        cur.execute(
            """
            SELECT address_id
            FROM addresses
            WHERE zip = %s
              AND house = %s
              AND soundex(street) = %s
            LIMIT 1
            """,
            (zip_code, num, sx),
        )
        hit = cur.fetchone()
        if hit:
            cur.execute(
                """
                UPDATE transactions
                SET address_id=%s, match_type='soundex', confidence=0.6
                WHERE id=%s
                """,
                (hit[0], tid),
            )
    conn.commit()
    conn.close()

if __name__ == "__main__":
    soundex_match()
