#!/usr/bin/env python3
import csv
import sqlite3
import glob

conn = sqlite3.connect('polk_county.db')
cursor = conn.cursor()

# Create sales table with foreign key to properties
cursor.execute('''
CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
    quality1 TEXT,
    quality2 TEXT,
    analysis_quality TEXT,
    total_full INTEGER,
    total_living_area INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    year_built INTEGER,
    school_district TEXT,
    platname TEXT,
    yr INTEGER,
    FOREIGN KEY (dp) REFERENCES properties(dp)
)
''')

cursor.execute('CREATE INDEX IF NOT EXISTS idx_sales_dp ON sales(dp)')
cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_sales_unique ON sales(dp, sale_date, book, pg)')

def parse_int(val):
    if val == '' or val is None:
        return None
    try:
        return int(val)
    except ValueError:
        return None

# Find all year CSV files (1990-2026)
sale_files = sorted(glob.glob('[0-9][0-9][0-9][0-9].csv'))
total_loaded = 0

for filepath in sale_files:
    year = filepath.replace('.csv', '')
    print(f"Loading {filepath}...", end=' ')
    count = 0

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            cursor.execute('''
                INSERT OR REPLACE INTO sales (
                    dp, gp, sale_date, book, pg, instrument, price, address, zip,
                    seller, buyer, quality1, quality2, analysis_quality,
                    total_full, total_living_area, bedrooms, bathrooms,
                    year_built, school_district, platname, yr
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
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
                row.get('quality1', '').strip() or None,
                row.get('quality2', '').strip() or None,
                row.get('analysis_quality', '').strip() or None,
                parse_int(row.get('total_full', '')),
                parse_int(row.get('total_living_area', '')),
                parse_int(row.get('bedrooms', '')),
                parse_int(row.get('bathrooms', '')),
                parse_int(row.get('year_built', '')),
                row.get('school_district', '').strip() or None,
                row.get('platname', '').strip() or None,
                parse_int(row.get('yr', '')) or parse_int(year)
            ))
            count += 1

    print(f"{count} records")
    total_loaded += count

conn.commit()
final_count = cursor.execute('SELECT COUNT(*) FROM sales').fetchone()[0]
print(f"\nTotal: {final_count} sale records loaded")
conn.close()
