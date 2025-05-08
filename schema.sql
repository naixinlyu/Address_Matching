CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;

DROP TABLE IF EXISTS addresses CASCADE;

CREATE TABLE addresses (
    address_id      SERIAL PRIMARY KEY, 
    -- raw columns straight from 11211 Addresses.csv 
    hhid            TEXT,
    fname           TEXT,
    mname           TEXT,
    lname           TEXT,
    suffix          TEXT,
    address         TEXT,
    house           TEXT,   -- street number
    predir          TEXT,
    street          TEXT,   -- street name
    strtype         TEXT,   -- suffix (St, Ave, Rd…)
    postdir         TEXT,
    apttype         TEXT,
    aptnbr          TEXT,
    city            TEXT,
    state           CHAR(2),
    zip             CHAR(10),
    latitude        NUMERIC,
    longitude       NUMERIC,
    homeownercd     TEXT,

    -- parsed / normalized fields (filled later by parse.py) 
    street_number   TEXT,
    street_name     TEXT,
    street_suffix   TEXT,
    unit_number     TEXT
);

DROP TABLE IF EXISTS transactions CASCADE;

CREATE TABLE transactions (
    -- columns taken verbatim from transactions_2_11211.csv
    id                         TEXT,
    status                     TEXT,
    price                      NUMERIC,
    bedrooms                   INTEGER,
    bathrooms                  INTEGER,
    square_feet                INTEGER,
    address_line_1             TEXT,
    address_line_2             TEXT,
    city                       TEXT,
    state                      CHAR(2),
    zip_code                   CHAR(10),
    property_type              TEXT,
    year_built                 INTEGER,
    presented_by               TEXT,
    brokered_by                TEXT,
    presented_by_mobile        TEXT,
    mls                        TEXT,
    listing_office_id          TEXT,
    listing_agent_id           TEXT,
    created_at                 TIMESTAMP,
    updated_at                 TIMESTAMP,
    open_house                 TEXT,
    latitude                   NUMERIC,
    longitude                  NUMERIC,
    email                      TEXT,
    list_date                  DATE,
    pending_date               DATE,
    presented_by_first_name    TEXT,
    presented_by_last_name     TEXT,
    presented_by_middle_name   TEXT,
    presented_by_suffix        TEXT,
    geog                       TEXT,

    street_number   TEXT,
    street_name     TEXT,
    street_suffix   TEXT,
    unit_number     TEXT,
    zip             CHAR(10),

    address_id      INTEGER,   
    match_type      TEXT,      -- exact | fuzzy | soundex | api
    confidence      NUMERIC,
    failure_reason  TEXT
);

DROP TABLE IF EXISTS transactions_raw;

CREATE TABLE transactions_raw (LIKE transactions);
ALTER TABLE transactions_raw
    ALTER COLUMN price            TYPE TEXT,
    ALTER COLUMN bedrooms         TYPE TEXT,
    ALTER COLUMN bathrooms        TYPE TEXT,
    ALTER COLUMN square_feet      TYPE TEXT,
    ALTER COLUMN year_built       TYPE TEXT,
    ALTER COLUMN latitude         TYPE TEXT,
    ALTER COLUMN longitude        TYPE TEXT;

-- Indexes
--  Fuzzy street‑name searches on canonical table
CREATE INDEX idx_addr_trgm  ON addresses USING gin(street gin_trgm_ops);
-- "Blocking" index for quick candidate pull by ZIP + house number
CREATE INDEX idx_addr_block ON addresses(zip, house);
-- Blocking index for transactions (populated later) 
CREATE INDEX idx_txn_block  ON transactions(zip, street_number);