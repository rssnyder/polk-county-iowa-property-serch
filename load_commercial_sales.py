#!/usr/bin/env python3
"""
Load commercial sales data from CSV into SQLite.
Supports incremental updates via INSERT OR REPLACE on (dp, sale_date, book, pg).

Usage: python3 load_commercial_sales.py [csv_file] [db_file]
  Defaults: raw/com/POLKCOUNTY_COM_SALES_*.csv -> polk_county.db
"""
import csv
import sqlite3
import glob
import sys

# Default paths
CSV_PATTERN = 'raw/com/POLKCOUNTY_COM_SALES_*.csv'
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

# Create commercial_sales table
cursor.execute('''
CREATE TABLE IF NOT EXISTS commercial_sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dp TEXT,
    gp TEXT,
    occupancy_group TEXT,
    occupancy_group_descr TEXT,
    jurisdiction TEXT,
    nbhd TEXT,
    sale_date TEXT,
    book TEXT,
    pg TEXT,
    instrument TEXT,
    price INTEGER,
    price_pp INTEGER,
    address TEXT,
    zip TEXT,
    seller TEXT,
    buyer TEXT,
    land_full INTEGER,
    bldg_full INTEGER,
    total_full INTEGER,
    class TEXT,
    quality TEXT,
    occupancy TEXT,
    occupancy_descr TEXT,
    primary_group TEXT,
    secondary_group TEXT,
    percent_primary INTEGER,
    percent_secondary INTEGER,
    year_built INTEGER,
    number_units INTEGER,
    bldg_class TEXT,
    grade TEXT,
    condition TEXT,
    gross_area INTEGER,
    total_story_height INTEGER,
    ground_floor_area INTEGER,
    perimeter INTEGER,
    wall_height INTEGER,
    bsmt_unfinished_area INTEGER,
    bsmt_finished_area INTEGER,
    bsmt_parking_area INTEGER,
    finished_area INTEGER,
    unfinished_area INTEGER,
    mezzanine_finished_area INTEGER,
    mezzanine_unfinished_area INTEGER,
    air_cond_area INTEGER,
    sprinkle_area INTEGER,
    number_passenger_stops INTEGER,
    number_freight_stops INTEGER,
    land_area INTEGER,
    detached_structs TEXT,
    occupant TEXT,
    dps TEXT,
    gps TEXT,
    zoning TEXT,
    platname TEXT,
    legal_all_parcels TEXT,
    school_district TEXT,
    initial_entry_date TEXT,
    yr INTEGER,
    FOREIGN KEY (dp) REFERENCES commercial_properties(dp)
)
''')

cursor.execute('CREATE INDEX IF NOT EXISTS idx_com_sales_dp ON commercial_sales(dp)')
cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_com_sales_unique ON commercial_sales(dp, sale_date, book, pg)')

def parse_int(val):
    if val == '' or val is None:
        return None
    try:
        return int(float(val))
    except ValueError:
        return None

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    count = 0

    for row in reader:
        cursor.execute('''
            INSERT OR REPLACE INTO commercial_sales (
                dp, gp, occupancy_group, occupancy_group_descr, jurisdiction, nbhd,
                sale_date, book, pg, instrument, price, price_pp, address, zip,
                seller, buyer, land_full, bldg_full, total_full, class, quality,
                occupancy, occupancy_descr, primary_group, secondary_group,
                percent_primary, percent_secondary, year_built, number_units,
                bldg_class, grade, condition, gross_area, total_story_height,
                ground_floor_area, perimeter, wall_height, bsmt_unfinished_area,
                bsmt_finished_area, bsmt_parking_area, finished_area, unfinished_area,
                mezzanine_finished_area, mezzanine_unfinished_area, air_cond_area,
                sprinkle_area, number_passenger_stops, number_freight_stops, land_area,
                detached_structs, occupant, dps, gps, zoning, platname,
                legal_all_parcels, school_district, initial_entry_date, yr
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                      ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            row.get('dp', '').strip() or None,
            row.get('gp', '').strip() or None,
            row.get('occupancy_group', '').strip() or None,
            row.get('occupancy_group_descr', '').strip() or None,
            row.get('jurisdiction', '').strip() or None,
            row.get('nbhd', '').strip() or None,
            row.get('sale_date', '').strip() or None,
            row.get('book', '').strip() or None,
            row.get('pg', '').strip() or None,
            row.get('instrument', '').strip() or None,
            parse_int(row.get('price', '')),
            parse_int(row.get('price_pp', '')),
            row.get('address', '').strip() or None,
            row.get('zip', '').strip() or None,
            row.get('seller', '').strip() or None,
            row.get('buyer', '').strip() or None,
            parse_int(row.get('land_full', '')),
            parse_int(row.get('bldg_full', '')),
            parse_int(row.get('total_full', '')),
            row.get('class', '').strip() or None,
            row.get('quality', '').strip() or None,
            row.get('occupancy', '').strip() or None,
            row.get('occupancy_descr', '').strip() or None,
            row.get('primary_group', '').strip() or None,
            row.get('secondary_group', '').strip() or None,
            parse_int(row.get('percent_primary', '')),
            parse_int(row.get('percent_secondary', '')),
            parse_int(row.get('year_built', '')),
            parse_int(row.get('number_units', '')),
            row.get('bldg_class', '').strip() or None,
            row.get('grade', '').strip() or None,
            row.get('condition', '').strip() or None,
            parse_int(row.get('gross_area', '')),
            parse_int(row.get('total_story_height', '')),
            parse_int(row.get('ground_floor_area', '')),
            parse_int(row.get('perimeter', '')),
            parse_int(row.get('wall_height', '')),
            parse_int(row.get('bsmt_unfinished_area', '')),
            parse_int(row.get('bsmt_finished_area', '')),
            parse_int(row.get('bsmt_parking_area', '')),
            parse_int(row.get('finished_area', '')),
            parse_int(row.get('unfinished_area', '')),
            parse_int(row.get('mezzanine_finished_area', '')),
            parse_int(row.get('mezzanine_unfinished_area', '')),
            parse_int(row.get('air_cond_area', '')),
            parse_int(row.get('sprinkle_area', '')),
            parse_int(row.get('number_passenger_stops', '')),
            parse_int(row.get('number_freight_stops', '')),
            parse_int(row.get('land_area', '')),
            row.get('detached_structs', '').strip() or None,
            row.get('occupant', '').strip() or None,
            row.get('dps', '').strip() or None,
            row.get('gps', '').strip() or None,
            row.get('zoning', '').strip() or None,
            row.get('platname', '').strip() or None,
            row.get('legal_all_parcels', '').strip() or None,
            row.get('school_district', '').strip() or None,
            row.get('initial_entry_date', '').strip() or None,
            parse_int(row.get('yr', ''))
        ))
        count += 1

conn.commit()
final_count = cursor.execute('SELECT COUNT(*) FROM commercial_sales').fetchone()[0]
print(f"Loaded {count} records from CSV")
print(f"Total commercial sales in database: {final_count}")
conn.close()
