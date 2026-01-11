#First download the given user.csv file and run this code then it will work
#this code is google colab oriented, you can make changes according in your system if not using colab
from google.colab import files

# Upload files from your computer
uploaded = files.upload()

import csv
import sqlite3
import os

DB_FILE = "users.db"
CSV_FILE = "user.csv"
TABLE_NAME = "users"

# 1. Connect and create table (no UNIQUE constraint on email)
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

cur.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL -- Allows duplicates
)
""")
conn.commit()

# 2. Read CSV and insert data
if not os.path.exists(CSV_FILE):
    print(f"Error: CSV file '{CSV_FILE}' not found.")
else:
    print(f"Importing data from {CSV_FILE}...")
    with open(CSV_FILE, "r") as file:
        reader = csv.DictReader(file)
        records_to_insert = [(row.get("name", "Unknown"), row.get("email", "")) for row in reader if row.get("email")]
        
        try:
            cur.executemany(f"INSERT INTO {TABLE_NAME} (name, email) VALUES (?, ?)", records_to_insert)
            conn.commit()
            print(f"Imported {len(records_to_insert)} records.")
        except Exception as e:
            print(f"Error during import: {e}")

# 3. Fetch and print all data

cur.execute(f"SELECT id, name, email FROM {TABLE_NAME}")
rows = cur.fetchall()

if rows:
    # Print headers
    headers = [col[0] for col in cur.description]
    print(f"| {' | '.join(headers)} |")
    print("-" * (sum(len(h) for h in headers) + len(headers) * 3 + 1))
    
    # Print data
    for row in rows:
        print(f"| {row[0]:<2} | {row[1]:<15} | {row[2]:<25} |")
else:
    print("Table is empty.")

# 4. Close connection
conn.close()
