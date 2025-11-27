from fastapi import FastAPI
import duckdb

app = FastAPI()

DB_PATH = "/app/northwind.duckdb"

@app.get("/")
def root():
    return {"status": "ok"}

@app.post("/run_query")
def run_query(body: dict):
    query = body["query"]
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute(query).df()
    return {"data": df.to_dict(orient="records")}