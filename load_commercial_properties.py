#!/usr/bin/env python3
"""
Load commercial property data from CSV into SQLite.
Supports incremental updates via INSERT OR REPLACE on dp (parcel ID).

Usage: python3 load_commercial_properties.py [csv_file] [db_file]
  Defaults: raw/com/POLKCOUNTY_COM_*.csv -> polk_county.db
"""
import csv
import sqlite3
import glob
import sys

# Default paths
CSV_PATTERN = 'raw/com/POLKCOUNTY_COM_[0-9]*.csv'  # Matches POLKCOUNTY_COM_3-16-2026.csv but not SALES
DB_PATH = 'polk_county.db'

# Allow command line overrides
if len(sys.argv) > 1:
    CSV_PATTERN = sys.argv[1]
if len(sys.argv) > 2:
    DB_PATH = sys.argv[2]

# Find the CSV file
csv_files = glob.glob(CSV_PATTERN)
if not csv_files:
    # Try without pattern
    if '*' not in CSV_PATTERN and CSV_PATTERN.endswith('.csv'):
        csv_files = [CSV_PATTERN]
    else:
        print(f"No CSV files found matching: {CSV_PATTERN}")
        sys.exit(1)

csv_file = sorted(csv_files)[-1]  # Use most recent if multiple
print(f"Loading from: {csv_file}")
print(f"Database: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Column definitions
text_cols = [
    'occupancy_group', 'occupancy_group_descr', 'jurisdiction', 'nbhd', 'dp', 'gp',
    'address_line1', 'address_line2', 'house', 'house_portion', 'dir', 'street',
    'suffix', 'suffix_dir', 'unit_type', 'unit_number', 'bldg', 'city', 'state',
    'zip', 'zip4', 'class', 'class_descr', 'occupancy', 'occupancy_descr',
    'primary_group', 'secondary_group', 'bldg_class', 'grade', 'condition',
    'occupant', 'title_holder1', 'last_name_th1', 'first_name_th1', 'initial_th1',
    'transfer_th1', 'book_th1', 'pg_th1', 'title_holder2', 'last_name_th2',
    'first_name_th2', 'initial_th2', 'contract_buyer1', 'last_name_cb1',
    'first_name_cb1', 'initial_cb1', 'transfer_cb1', 'book_cb1', 'pg_cb1',
    'contract_buyer2', 'last_name_cb2', 'first_name_cb2', 'initial_cb2',
    'mail_line1', 'mail_line2', 'mail_house', 'mail_house_portion', 'mail_dir',
    'mail_street', 'mail_suffix', 'mail_suffix_dir', 'mail_unit_type',
    'mail_unit_number', 'mail_city', 'mail_state', 'mail_zip', 'mail_zip4',
    'mail_name', 'mail_last_name', 'mail_first_name', 'mail_initial',
    'mail_business', 'tif_descr', 'zoning', 'platname', 'legal', 'school_district'
]

int_cols = [
    'land_full', 'bldg_full', 'total_full', 'land_adj', 'bldg_adj', 'total_adj',
    'percent_primary', 'percent_secondary', 'year_built', 'number_units',
    'gross_area', 'total_story_height', 'ground_floor_area', 'perimeter',
    'wall_height', 'bsmt_unfinished_area', 'bsmt_finished_area', 'bsmt_parking_area',
    'finished_area', 'unfinished_area', 'mezzanine_finished_area',
    'mezzanine_unfinished_area', 'air_cond_area', 'sprinkle_area',
    'number_passenger_stops', 'number_freight_stops', 'land_area', 'tif'
]

real_cols = ['frontage', 'depth', 'x', 'y']

# Build CREATE TABLE statement
col_defs = ['id INTEGER PRIMARY KEY AUTOINCREMENT']
for col in text_cols:
    col_defs.append(f'{col} TEXT')
for col in int_cols:
    col_defs.append(f'{col} INTEGER')
for col in real_cols:
    col_defs.append(f'{col} REAL')

cursor.execute('CREATE TABLE IF NOT EXISTS commercial_properties (' + ', '.join(col_defs) + ')')
cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_com_prop_dp ON commercial_properties(dp)')

def parse_int(val):
    if val == '' or val is None:
        return None
    try:
        return int(float(val))  # Handle "100.0" style values
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
        cursor.execute(f'INSERT OR REPLACE INTO commercial_properties ({col_names}) VALUES ({placeholders})', values)
        count += 1

conn.commit()
final_count = cursor.execute('SELECT COUNT(*) FROM commercial_properties').fetchone()[0]
print(f"Loaded {count} records from CSV")
print(f"Total commercial properties in database: {final_count}")
conn.close()
