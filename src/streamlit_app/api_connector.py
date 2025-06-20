# src/streamlit_app/api_connector.py
import streamlit as st
import requests
import pandas as pd
import os
import logging

# Setup logger sederhana
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)

# Ambil URL base Flask API dari environment variable, default ke Docker Compose service name
FLASK_API_BASE_URL = os.environ.get("FLASK_API_BASE_URL", "http://flask-api:5000")

@st.cache_data(ttl=300) # Cache data selama 5 menit
def fetch_data_from_api(endpoint_path: str, params: dict = None):
    """Mengambil data dari endpoint Flask API."""
    full_url = f"{FLASK_API_BASE_URL}{endpoint_path}"
    try:
        logger.info(f"Fetching data from API: {full_url} with params: {params}")
        response = requests.get(full_url, params=params, timeout=30) # Timeout 30 detik
        response.raise_for_status()  # Akan error jika status code 4xx atau 5xx
        data = response.json()
        logger.info(f"Successfully fetched {len(data) if isinstance(data, list) else '1 record/object'} from {full_url}")
        return pd.DataFrame(data) # Langsung konversi ke DataFrame Pandas
    except requests.exceptions.Timeout:
        logger.error(f"Timeout when fetching data from API: {full_url}")
        st.error(f"Timeout saat mengambil data dari API: {endpoint_path}")
        return pd.DataFrame()
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred when fetching data from API: {full_url} - {http_err} - Response: {response.text}")
        st.error(f"HTTP error saat mengambil data dari API: {endpoint_path} - {http_err}")
        return pd.DataFrame()
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request exception occurred when fetching data from API: {full_url} - {req_err}")
        st.error(f"Kesalahan koneksi saat mengambil data dari API: {endpoint_path} - {req_err}")
        return pd.DataFrame()
    except ValueError as json_err: # Jika respons bukan JSON yang valid
        logger.error(f"JSON decode error when fetching data from API: {full_url} - {json_err} - Response: {response.text}")
        st.error(f"Gagal mem-parse respons JSON dari API: {endpoint_path}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"An unexpected error occurred when fetching data from API: {full_url} - {e}", exc_info=True)
        st.error(f"Terjadi kesalahan tak terduga saat mengambil data: {e}")
        return pd.DataFrame()