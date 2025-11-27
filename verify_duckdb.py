import duckdb

DB = "northwind.duckdb"

con = duckdb.connect(DB)

# 테이블 목록 확인
tables = con.execute("""
    SELECT table_name 
    FROM information_schema.tables
    WHERE table_schema='main';
""").fetchall()

print("Tables:", tables)

# 각 테이블 row 개수 확인
for (table_name,) in tables:
    cnt = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
    print(f"{table_name}: {cnt} rows")

# 샘플 데이터
sample = con.execute('SELECT * FROM "Orders" LIMIT 5').fetchdf()
print(sample)

con.close()
