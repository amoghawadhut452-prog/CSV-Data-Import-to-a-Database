import csv
import sqlite3

# Connect to SQLite database
conn = sqlite3.connect("users.db")
cur = conn.cursor()

# Create table
cur.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE
)
""")
conn.commit()

# Read CSV and insert into DB
csv_file = "user.csv"
with open(csv_file, "r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        name = row.get("name", "Unknown")
        email = row.get("email", "")
        try:
            cur.execute("INSERT INTO users (name, email) VALUES (?, ?)", (name, email))
        except sqlite3.IntegrityError:
            print(f"Skipped duplicate email: {email}")

conn.commit()
