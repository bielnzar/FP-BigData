import streamlit as st
import pandas as pd
from trino.dbapi import connect
from trino.exceptions import TrinoUserError
import os

# --- Konfigurasi Koneksi ---
TRINO_HOST = os.environ.get("TRINO_HOST", "localhost")
TRINO_PORT = int(os.environ.get("TRINO_PORT", 8082))
TRINO_USER = os.environ.get("TRINO_USER", "user")
FLASK_API_URL = os.environ.get("FLASK_API_URL", "http://localhost:5001")

@st.cache_data(ttl=600) # Cache data selama 10 menit untuk performa
def query_trino(query):
    """Menjalankan kueri di Trino dan mengembalikan hasilnya sebagai DataFrame."""
    try:
        conn = connect(
            host=TRINO_HOST,
            port=TRINO_PORT,
            user=TRINO_USER,
            catalog="hive",
            schema="gold"
        )
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)
    except TrinoUserError as e:
        if e.error_name == 'SCHEMA_NOT_FOUND':
            st.warning("Schema 'gold' belum ada. Silakan jalankan pipeline ETL 'silver_to_gold.py'.")
        elif e.error_name == 'TABLE_NOT_FOUND':
             st.warning(f"Tabel yang diperlukan belum ada. Pastikan pipeline ETL 'silver_to_gold.py' sudah berjalan.")
        else:
            st.error(f"Terjadi kesalahan pada Trino: {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Gagal mengambil data dari Trino: {e}")
        return pd.DataFrame()
