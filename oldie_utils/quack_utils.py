import duckdb
import glob

# Path to your JSON files (each file is a JSON array of dicts)
json_files = glob.glob("cultura_pages/*.json")

con = duckdb.connect(":memory:")

# Read all JSON files directly into DuckDB
con.execute("""
    CREATE OR REPLACE TABLE events AS
    SELECT * FROM read_json_auto($files, format='array')
""", {"files": json_files})

# Export to Parquet
con.execute("COPY events TO 'events.parquet' (FORMAT PARQUET)")

# Shape of table
n_rows = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
n_cols = len(con.execute("PRAGMA table_info(events)").fetchall())

# rows must match found events in main page
print("✅ Data loaded into DuckDB and exported to events.parquet")
print(f"📊 Shape of data: {n_rows} rows × {n_cols} columns")

# Preview first few rows
print("\n🔎 Preview:")
print(con.execute("SELECT * FROM events LIMIT 5").fetchdf())
