#!/usr/bin/env python3
"""
Load residential property data from CSV into SQLite.
Supports incremental updates via INSERT OR REPLACE on dp (parcel ID).

Usage: python3 load_residential_properties.py [csv_file] [db_file]
  Defaults: raw/res/POLKCOUNTY_*.csv (not SALES) -> polk_county.db
"""
import csv
import sqlite3
import glob
import sys

# Default paths
CSV_PATTERN = 'raw/res/POLKCOUNTY_[0-9]*.csv'
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

# Column definitions
text_cols = [
    'jurisdiction', 'nbhd', 'pocket', 'dp', 'gp', 'address_line1', 'address_line2',
    'house', 'house_portion', 'dir', 'street', 'suffix', 'suffix_dir', 'unit_type',
    'unit_number', 'bldg', 'city', 'state', 'zip', 'zip4', 'class', 'class_descr',
    'occupancy', 'residence_type', 'bldg_style', 'exterior_wall_type', 'roof_type',
    'roof_material', 'foundation', 'fin_bsmt_qual1', 'fin_bsmt_qual2', 'condition',
    'grade', 'heating', 'condo_unit_address', 'commercial_occupancy', 'detached_structs',
    'title_holder1', 'last_name_th1', 'first_name_th1', 'initial_th1', 'transfer_th1',
    'book_th1', 'pg_th1', 'title_holder2', 'last_name_th2', 'first_name_th2', 'initial_th2',
    'contract_buyer1', 'last_name_cb1', 'first_name_cb1', 'initial_cb1', 'transfer_cb1',
    'book_cb1', 'pg_cb1', 'contract_buyer2', 'last_name_cb2', 'first_name_cb2', 'initial_cb2',
    'mail_line1', 'mail_line2', 'mail_house', 'mail_house_portion', 'mail_dir', 'mail_street',
    'mail_suffix', 'mail_suffix_dir', 'mail_unit_type', 'mail_unit_number', 'mail_city',
    'mail_state', 'mail_zip', 'mail_zip4', 'mail_name', 'mail_last_name', 'mail_first_name',
    'mail_initial', 'mail_business', 'tif_descr', 'platname', 'legal', 'school_district'
]

int_cols = [
    'land_full', 'bldg_full', 'total_full', 'land_adj', 'bldg_adj', 'total_adj',
    'land_sf', 'percent_brick', 'main_living_area', 'upper_living_area', 'fin_attic_area',
    'total_living_area', 'unfin_attic_area', 'basement_area', 'fin_bsmt_area_tot',
    'bsmt_walkout', 'bsmt_gar_capacity', 'att_garage_area', 'garage_brick',
    'open_porch_area', 'enclose_porch_area', 'patio_area', 'deck_area', 'canopy_area',
    'veneer_area', 'carport_area', 'fin_bsmt_area1', 'fin_bsmt_area2', 'bathrooms',
    'toilet_rooms', 'extra_fixtures', 'whirlpools', 'hottubs', 'saunas', 'fireplaces',
    'bedrooms', 'rooms', 'families', 'year_built', 'year_remodel', 'eff_year_built',
    'air_conditioning', 'percent_complete', 'condo_fin_liv_area', 'condo_year_built',
    'commercial_area', 'tif'
]

real_cols = ['land_acres', 'frontage', 'depth', 'x', 'y']

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Build CREATE TABLE statement
col_defs = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
for col in text_cols:
    col_defs.append(f'{col} TEXT')
for col in int_cols:
    col_defs.append(f'{col} INTEGER')
for col in real_cols:
    col_defs.append(f'{col} REAL')

cursor.execute('CREATE TABLE IF NOT EXISTS properties (' + ', '.join(col_defs) + ')')
cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_dp ON properties(dp)')

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

all_cols = text_cols + int_cols + real_cols

with open(csv_file, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    count = 0

    for row in reader:
        values = []
        for col in text_cols:
            values.append(row.get(col, '').strip() or None)
        for col in int_cols:
            values.append(parse_int(row.get(col, '')))
        for col in real_cols:
            values.append(parse_float(row.get(col, '')))

        placeholders = ', '.join(['?'] * len(all_cols))
        col_names = ', '.join(all_cols)
        cursor.execute(f'INSERT OR REPLACE INTO properties ({col_names}) VALUES ({placeholders})', values)
        count += 1

conn.commit()
final_count = cursor.execute('SELECT COUNT(*) FROM properties').fetchone()[0]
print(f"Loaded {count} records from CSV")
print(f"Total residential properties in database: {final_count}")
conn.close()
