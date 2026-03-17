#!/usr/bin/env python3
"""
Load agricultural sales data from CSV into SQLite.
Supports incremental updates via INSERT OR REPLACE on (dp, sale_date, book, pg).

Usage: python3 load_agricultural_sales.py [csv_file] [db_file]
  Defaults: raw/agr/POLKCOUNTY_AGR_SALES_*.csv -> polk_county.db
"""
import csv
import sqlite3
import glob
import sys

# Default paths
CSV_PATTERN = 'raw/agr/POLKCOUNTY_AGR_SALES_*.csv'
DB_PATH = 'polk_county.db'

# Allow command line overrides
if len(sys.argv) > 1:
    CSV_PATTERN = sys.argv[1]
if len(sys.argv) > 2:
    DB_PATH = sys.argv[2]

# Find the CSV file
csv_files = glob.glob(CSV_PATTERN)
if not csv_files:
    if '*' not in CSV_PATTERN and CSV_PATTERN.endswith('.csv'):
        csv_files = [CSV_PATTERN]
    else:
        print(f"No CSV files found matching: {CSV_PATTERN}")
        sys.exit(1)

csv_file = sorted(csv_files)[-1]
print(f"Loading from: {csv_file}")
print(f"Database: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Create agricultural_sales table
cursor.execute('''
CREATE TABLE IF NOT EXISTS agricultural_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    jurisdiction TEXT,
    nbhd TEXT,
    dp TEXT,
    gp TEXT,
    sale_date TEXT,
    book TEXT,
    pg TEXT,
    instrument TEXT,
    price INTEGER,
    address TEXT,
    zip TEXT,
    seller TEXT,
    buyer TEXT,
    land_full INTEGER,
    bldg_full INTEGER,
    total_full INTEGER,
    quality1 TEXT,
    quality2 TEXT,
    analysis_quality TEXT,
    dps TEXT,
    gps TEXT,
    land_sf INTEGER,
    land_acres REAL,
    occupancy TEXT,
    residence_type TEXT,
    bldg_style TEXT,
    exterior_wall_type TEXT,
    percent_brick INTEGER,
    roof_type TEXT,
    roof_material TEXT,
    main_living_area INTEGER,
    upper_living_area INTEGER,
    fin_attic_area INTEGER,
    total_living_area INTEGER,
    unfin_attic_area INTEGER,
    foundation TEXT,
    basement_area INTEGER,
    fin_bsmt_area_tot INTEGER,
    bsmt_walkout INTEGER,
    bsmt_gar_capacity INTEGER,
    att_garage_area INTEGER,
    garage_brick INTEGER,
    open_porch_area INTEGER,
    enclose_porch_area INTEGER,
    patio_area INTEGER,
    deck_area INTEGER,
    canopy_area INTEGER,
    veneer_area INTEGER,
    carport_area INTEGER,
    fin_bsmt_area1 INTEGER,
    fin_bsmt_qual1 TEXT,
    fin_bsmt_area2 INTEGER,
    fin_bsmt_qual2 TEXT,
    bathrooms INTEGER,
    toilet_rooms INTEGER,
    extra_fixtures INTEGER,
    whirlpools INTEGER,
    hottubs INTEGER,
    saunas INTEGER,
    fireplaces INTEGER,
    bedrooms INTEGER,
    rooms INTEGER,
    families INTEGER,
    year_built INTEGER,
    year_remodel INTEGER,
    eff_year_built INTEGER,
    condition TEXT,
    grade TEXT,
    heating TEXT,
    air_conditioning INTEGER,
    percent_complete INTEGER,
    detached_structs TEXT,
    platname TEXT,
    legal_all_parcels TEXT,
    school_district TEXT,
    initial_entry_date TEXT,
    yr INTEGER,
    FOREIGN KEY (dp) REFERENCES agricultural_properties(dp)
)
''')

cursor.execute('CREATE INDEX IF NOT EXISTS idx_agr_sales_dp ON agricultural_sales(dp)')
cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_agr_sales_unique ON agricultural_sales(dp, sale_date, book, pg)')

def parse_int(val):
    if val == '' or val is None:
        return None
    try:
        return int(float(val))
    except ValueError:
        return None

def parse_float(val):
    if val == '' or val is None:
        return None
    try:
        return float(val)
    except ValueError:
        return None

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    count = 0

    for row in reader:
        cursor.execute('''
            INSERT OR REPLACE INTO agricultural_sales (
                jurisdiction, nbhd, dp, gp, sale_date, book, pg, instrument, price,
                address, zip, seller, buyer, land_full, bldg_full, total_full,
                quality1, quality2, analysis_quality, dps, gps, land_sf, land_acres,
                occupancy, residence_type, bldg_style, exterior_wall_type, percent_brick,
                roof_type, roof_material, main_living_area, upper_living_area, fin_attic_area,
                total_living_area, unfin_attic_area, foundation, basement_area, fin_bsmt_area_tot,
                bsmt_walkout, bsmt_gar_capacity, att_garage_area, garage_brick, open_porch_area,
                enclose_porch_area, patio_area, deck_area, canopy_area, veneer_area, carport_area,
                fin_bsmt_area1, fin_bsmt_qual1, fin_bsmt_area2, fin_bsmt_qual2, bathrooms,
                toilet_rooms, extra_fixtures, whirlpools, hottubs, saunas, fireplaces, bedrooms,
                rooms, families, year_built, year_remodel, eff_year_built, condition, grade,
                heating, air_conditioning, percent_complete, detached_structs, platname,
                legal_all_parcels, school_district, initial_entry_date, yr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('jurisdiction', '').strip() or None,
            row.get('nbhd', '').strip() or None,
            row.get('dp', '').strip() or None,
            row.get('gp', '').strip() or None,
            row.get('sale_date', '').strip() or None,
            row.get('book', '').strip() or None,
            row.get('pg', '').strip() or None,
            row.get('instrument', '').strip() or None,
            parse_int(row.get('price', '')),
            row.get('address', '').strip() or None,
            row.get('zip', '').strip() or None,
            row.get('seller', '').strip() or None,
            row.get('buyer', '').strip() or None,
            parse_int(row.get('land_full', '')),
            parse_int(row.get('bldg_full', '')),
            parse_int(row.get('total_full', '')),
            row.get('quality1', '').strip() or None,
            row.get('quality2', '').strip() or None,
            row.get('analysis_quality', '').strip() or None,
            row.get('dps', '').strip() or None,
            row.get('gps', '').strip() or None,
            parse_int(row.get('land_sf', '')),
            parse_float(row.get('land_acres', '')),
            row.get('occupancy', '').strip() or None,
            row.get('residence_type', '').strip() or None,
            row.get('bldg_style', '').strip() or None,
            row.get('exterior_wall_type', '').strip() or None,
            parse_int(row.get('percent_brick', '')),
            row.get('roof_type', '').strip() or None,
            row.get('roof_material', '').strip() or None,
            parse_int(row.get('main_living_area', '')),
            parse_int(row.get('upper_living_area', '')),
            parse_int(row.get('fin_attic_area', '')),
            parse_int(row.get('total_living_area', '')),
            parse_int(row.get('unfin_attic_area', '')),
            row.get('foundation', '').strip() or None,
            parse_int(row.get('basement_area', '')),
            parse_int(row.get('fin_bsmt_area_tot', '')),
            parse_int(row.get('bsmt_walkout', '')),
            parse_int(row.get('bsmt_gar_capacity', '')),
            parse_int(row.get('att_garage_area', '')),
            parse_int(row.get('garage_brick', '')),
            parse_int(row.get('open_porch_area', '')),
            parse_int(row.get('enclose_porch_area', '')),
            parse_int(row.get('patio_area', '')),
            parse_int(row.get('deck_area', '')),
            parse_int(row.get('canopy_area', '')),
            parse_int(row.get('veneer_area', '')),
            parse_int(row.get('carport_area', '')),
            parse_int(row.get('fin_bsmt_area1', '')),
            row.get('fin_bsmt_qual1', '').strip() or None,
            parse_int(row.get('fin_bsmt_area2', '')),
            row.get('fin_bsmt_qual2', '').strip() or None,
            parse_int(row.get('bathrooms', '')),
            parse_int(row.get('toilet_rooms', '')),
            parse_int(row.get('extra_fixtures', '')),
            parse_int(row.get('whirlpools', '')),
            parse_int(row.get('hottubs', '')),
            parse_int(row.get('saunas', '')),
            parse_int(row.get('fireplaces', '')),
            parse_int(row.get('bedrooms', '')),
            parse_int(row.get('rooms', '')),
            parse_int(row.get('families', '')),
            parse_int(row.get('year_built', '')),
            parse_int(row.get('year_remodel', '')),
            parse_int(row.get('eff_year_built', '')),
            row.get('condition', '').strip() or None,
            row.get('grade', '').strip() or None,
            row.get('heating', '').strip() or None,
            parse_int(row.get('air_conditioning', '')),
            parse_int(row.get('percent_complete', '')),
            row.get('detached_structs', '').strip() or None,
            row.get('platname', '').strip() or None,
            row.get('legal_all_parcels', '').strip() or None,
            row.get('school_district', '').strip() or None,
            row.get('initial_entry_date', '').strip() or None,
            parse_int(row.get('yr', ''))
        ))
        count += 1

conn.commit()
final_count = cursor.execute('SELECT COUNT(*) FROM agricultural_sales').fetchone()[0]
print(f"Loaded {count} records from CSV")
print(f"Total agricultural sales in database: {final_count}")
conn.close()
