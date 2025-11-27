from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb

DB = "northwind.duckdb"

app = FastAPI(title="Northwind DuckDB API")

# DuckDB 연결 (read_only 모드 권장)
con = duckdb.connect(DB, read_only=True)


class QueryRequest(BaseModel):
    query: str


@app.post("/run_query")
def run_query(req: QueryRequest):
    try:
        df = con.execute(req.query).fetchdf()
        return {
            "columns": list(df.columns),
            "rows": df.to_dict(orient="records")
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
