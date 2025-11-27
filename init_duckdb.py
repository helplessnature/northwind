import sqlite3
import duckdb
from charset_normalizer import from_bytes
import os

SQL_FILE = "northwind.sqlite3.sql"
SQLITE_DB = "northwind.sqlite"
DUCK_DB = "northwind.duckdb"

# 1) Load SQL as text with encoding auto-detection
with open(SQL_FILE, "rb") as f:
    raw = f.read()

result = from_bytes(raw).best()
encoding = result.encoding
print("Detected encoding:", encoding)

sql_text = raw.decode(encoding)

# 2) Create SQLite DB from SQL dump
if os.path.exists(SQLITE_DB):
    os.remove(SQLITE_DB)

sqlite_con = sqlite3.connect(SQLITE_DB)
sqlite_con.executescript(sql_text)
sqlite_con.close()

print("SQLite DB created successfully!")

# 3) Migrate to DuckDB
if os.path.exists(DUCK_DB):
    os.remove(DUCK_DB)

con = duckdb.connect(DUCK_DB)

con.execute("INSTALL sqlite;")
con.execute("LOAD sqlite;")

# attach SQLite DB
con.execute(f"ATTACH '{SQLITE_DB}' AS sqlite_db;")

# get tables (SQLite side)
tables = con.execute("""
    SELECT name
    FROM main.sqlite_master
    WHERE type='table';
""").fetchall()

print("Tables found:", tables)

# copy tables to DuckDB
for (table_name,) in tables:
    safe_name = table_name.replace('"', '""')   # escape double-quote inside table name
    print(f"Copying table: {table_name}")

    con.execute(
        f'CREATE TABLE "{safe_name}" AS SELECT * FROM sqlite_db.main."{safe_name}";'
    )

con.close()

print("DuckDB migration completed! Saved as:", DUCK_DB)
