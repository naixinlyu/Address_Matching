"""
ingest.py  –  robust loader for the 11211 datasets.

Steps
1. TEMP addresses_tmp (no address_id)   ← COPY Addresses.csv
2. INSERT‑SELECT into real addresses    (casts, NULLIF)
3. TRUNCATE transactions_raw            ← COPY Transactions CSV
4. INSERT‑SELECT into typed transactions (casts, NULLIF)
"""

import argparse, os, psycopg2, sys, time


ap = argparse.ArgumentParser()
ap.add_argument("--transactions", required=True)
ap.add_argument("--addresses",    required=True)
args = ap.parse_args()

conn = psycopg2.connect(
    host=os.getenv("PGHOST", "localhost"),
    user=os.getenv("PGUSER", "addrmatch"),
    password=os.getenv("PGPASSWORD", "pwd"),
    dbname=os.getenv("PGDATABASE", "addrdb"),
)
cur = conn.cursor()


#  Addresses
print("COPY → TEMP addresses_tmp …")

cur.execute("""
    CREATE TEMP TABLE addresses_tmp (
      hhid TEXT,fname TEXT,mname TEXT,lname TEXT,suffix TEXT,address TEXT,
      house TEXT,predir TEXT,street TEXT,strtype TEXT,postdir TEXT,
      apttype TEXT,aptnbr TEXT,city TEXT,state CHAR(2),zip CHAR(10),
      latitude TEXT,longitude TEXT,homeownercd TEXT
    );
""")

addr_cols = (
    "hhid,fname,mname,lname,suffix,address,house,predir,street,strtype,"
    "postdir,apttype,aptnbr,city,state,zip,latitude,longitude,homeownercd"
)
with open(args.addresses, encoding="utf8") as f:
    cur.copy_expert(
        f"COPY addresses_tmp({addr_cols}) FROM STDIN WITH CSV HEADER NULL ''",
        f,
    )
conn.commit()

print("INSERT → addresses …")
cur.execute("TRUNCATE TABLE addresses;")
cur.execute(
    """
    INSERT INTO addresses (
      hhid,fname,mname,lname,suffix,address,house,predir,street,strtype,
      postdir,apttype,aptnbr,city,state,zip,latitude,longitude,homeownercd
    )
    SELECT
      hhid,fname,mname,lname,suffix,address,house,predir,street,strtype,
      postdir,apttype,aptnbr,city,state,zip,
      NULLIF(NULLIF(latitude ,'NULL'),'')::numeric,
      NULLIF(NULLIF(longitude,'NULL'),'')::numeric,
      homeownercd
    FROM addresses_tmp;
"""
)
conn.commit()
print("Addresses loaded\n")

# Transactions
print("COPY → transactions_raw …")

cur.execute("TRUNCATE TABLE transactions_raw;")

txn_cols = (
    "id,status,price,bedrooms,bathrooms,square_feet,address_line_1,"
    "address_line_2,city,state,zip_code,property_type,year_built,"
    "presented_by,brokered_by,presented_by_mobile,mls,listing_office_id,"
    "listing_agent_id,created_at,updated_at,open_house,latitude,longitude,"
    "email,list_date,pending_date,presented_by_first_name,"
    "presented_by_last_name,presented_by_middle_name,presented_by_suffix,geog"
)
with open(args.transactions, encoding="utf8") as f:
    cur.copy_expert(
        f"COPY transactions_raw({txn_cols}) FROM STDIN WITH CSV HEADER NULL 'NULL'",
        f,
    )
conn.commit()

print("INSERT → typed transactions …")
cur.execute("TRUNCATE TABLE transactions;")
cur.execute(
    f"""
INSERT INTO transactions ({txn_cols})
SELECT
  id,status,
  NULLIF(NULLIF(price,'NULL'),'')::numeric,
  NULLIF(NULLIF(bedrooms,'NULL'),'')::int,
  NULLIF(NULLIF(bathrooms,'NULL'),'')::int,
  NULLIF(NULLIF(square_feet,'NULL'),'')::int,
  address_line_1,address_line_2,city,state,zip_code,property_type,
  NULLIF(NULLIF(year_built,'NULL'),'')::int,
  presented_by,brokered_by,presented_by_mobile,mls,
  listing_office_id,listing_agent_id,
  NULLIF(NULLIF(created_at,'NULL'),'')::timestamp,
  NULLIF(NULLIF(updated_at,'NULL'),'')::timestamp,
  open_house,
  NULLIF(NULLIF(latitude ,'NULL'),'')::numeric,
  NULLIF(NULLIF(longitude,'NULL'),'')::numeric,
  email,
  NULLIF(NULLIF(list_date,'NULL'),'')::date,
  NULLIF(NULLIF(pending_date,'NULL'),'')::date,
  presented_by_first_name,presented_by_last_name,
  presented_by_middle_name,presented_by_suffix,geog
FROM transactions_raw;
"""
)
conn.commit()
print("Transactions loaded")

cur.close(); conn.close()
print("Ingest completed without errors.")

