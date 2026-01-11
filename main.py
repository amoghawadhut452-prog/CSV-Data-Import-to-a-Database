#First download the given user.csv file and run this code then it will work
#this code is google colab oriented, you can make changes according in your system if not using colab
from google.colab import files

# Upload files from your computer
uploaded = files.upload()


import csv
import sqlite3
import os

# Configuration
DB_NAME = "users.db"
TABLE_NAME = "users"
CSV_FILE = "user.csv"


# Connect to SQLite database
conn = sqlite3.connect(DB_NAME)
cur = conn.cursor()

def remove_unique_constraint(cursor, table_name):
    """
    Checks for and removes the UNIQUE constraint on the 'email' column 
    by recreating the table if the constraint is found.
    """
    print(f"Checking {table_name} table structure...")

    # Get the CREATE statement for the table
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    create_sql = cursor.fetchone()
    
    # Check if the table exists
    if not create_sql:
        print(f"Table {table_name} does not exist. Creating it without UNIQUE constraint.")
        # Create table without the UNIQUE constraint
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL
        )
        """)
        conn.commit()
        return

    create_sql = create_sql[0]

    # Simple check for the 'UNIQUE' keyword on the email column
    if "email TEXT NOT NULL UNIQUE" in create_sql:
        print("UNIQUE constraint found on 'email'. Rebuilding table...")
        
        OLD_TABLE_NAME = f"{table_name}_old_unique"
        

        cursor.execute(f"ALTER TABLE {table_name} RENAME TO {OLD_TABLE_NAME}")
 
        cursor.execute(f"""
        CREATE TABLE {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            email TEXT NOT NULL -- UNIQUE constraint removed here
        )
        """)
        
        # 3. Copy the data from the old table to the new one
        cursor.execute(f"INSERT INTO {table_name} (name, email) SELECT name, email FROM {OLD_TABLE_NAME}")
        
        # 4. Drop the old table (optional, but cleans up)
        cursor.execute(f"DROP TABLE {OLD_TABLE_NAME}")
        
        conn.commit()
        print(f"Table {table_name} successfully rebuilt. Duplicate emails are now allowed.")
    else:
        print("No UNIQUE constraint found on 'email'. Ready for import.")




# 1. Ensure the table is set up to allow duplicates
remove_unique_constraint(cur, TABLE_NAME)

# 2. Read CSV and insert into DB
if not os.path.exists(CSV_FILE):
    print(f"Error: The file '{CSV_FILE}' was not found. Please ensure it has been uploaded.")
else:
    print(f"Processing data from '{CSV_FILE}'...")
    with open(CSV_FILE, "r") as file:
      
        reader = csv.DictReader(file)
        
        # Prepare the list of records to insert (better performance)
        records_to_insert = []
        for row in reader:
            name = row.get("name", "Unknown")
            email = row.get("email", "")
            if email: # Only insert if email is not empty
                records_to_insert.append((name, email))
            else:
                print(f"Skipped record due to missing email: Name={name}")

        try:
            # Use executemany for efficient insertion
            cur.executemany(f"INSERT INTO {TABLE_NAME} (name, email) VALUES (?, ?)", records_to_insert)
            print(f"Successfully inserted {len(records_to_insert)} records.")
        except Exception as e:
            # This general exception catcher is less likely now that the UNIQUE constraint is gone
            print(f"An error occurred during insertion: {e}")

    conn.commit()

# Close the database connection
conn.close()
print("Database connection closed.")
