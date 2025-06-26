import os
import pandas as pd
import duckdb
import streamlit as st

MINIO_HOST = os.environ.get("MINIO_HOST", "minio")
MINIO_PORT = int(os.environ.get("MINIO_PORT", 9000))
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
FLASK_API_URL = os.environ.get("FLASK_API_URL", "http://flask-api:5001")

@st.cache_resource(ttl=3600)
def get_duckdb_connection():
    print(f"Connecting to DuckDB and configuring S3 endpoint at {MINIO_HOST}:{MINIO_PORT}")
    con = duckdb.connect(database=':memory:', read_only=False)
    
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    s3_endpoint = f"{MINIO_HOST}:{MINIO_PORT}"
    con.execute(f"SET s3_endpoint='{s3_endpoint}';")
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_access_key_id='{MINIO_ACCESS_KEY}';")
    con.execute(f"SET s3_secret_access_key='{MINIO_SECRET_KEY}';")
    con.execute("SET s3_use_ssl=false;")
    
    return con

def query_duckdb(query: str) -> pd.DataFrame:
    """Menjalankan kueri di DuckDB dan mengembalikan hasilnya sebagai DataFrame."""
    try:
        conn = get_duckdb_connection()
        df = conn.execute(query).fetchdf()
        return df
    except Exception as e:
        st.error(f"Gagal mengambil data dari DuckDB: {e}")
        return pd.DataFrame()
