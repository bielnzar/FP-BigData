import streamlit as st
import requests
import pandas as pd
from utils import FLASK_API_URL, query_duckdb

st.set_page_config(page_title="Model Prediktif", layout="wide")

st.title("🔮 Prediksi Angka Kematian")
st.markdown("""
Halaman ini memungkinkan Anda berinteraksi dengan model Machine Learning. 
Cukup pilih skenario utama, dan kami akan mengisi otomatis data sosio-ekonomi berdasarkan rata-rata negara yang dipilih untuk memberikan prediksi.
""")

try:
    health_check = requests.get(f"{FLASK_API_URL}/health")
    if health_check.status_code == 200 and health_check.json().get('status') == 'OK':
        st.success("Layanan Prediksi (Flask API) terhubung dan model berhasil dimuat.")
    else:
        st.error(f"Layanan Prediksi terhubung, tetapi model GAGAL dimuat. Pesan: {health_check.json().get('status')}")
        st.stop()
except requests.exceptions.ConnectionError:
    st.error("Gagal terhubung ke Layanan Prediksi (Flask API). Pastikan layanan 'flask-api' berjalan dan dapat diakses dari layanan dashboard.")
    st.stop()

@st.cache_data(ttl=3600)
def get_form_data():
    """Mengambil data yang diperlukan untuk mengisi form dari DuckDB."""
    countries_df = query_duckdb("SELECT DISTINCT Country FROM read_parquet('s3a://gold/country-health-summary/*.parquet') ORDER BY Country")
    disease_categories = ["Cardiovascular Diseases", "Infectious Diseases", "Cancers", "Respiratory Diseases"]
    age_groups = ["0-19", "20-39", "40-59", "60-79", "80+"]
    genders = ["Male", "Female"]
    
    if countries_df.empty:
        return None, None, None, None
    
    return countries_df['Country'].tolist(), disease_categories, age_groups, genders

countries, disease_categories, age_groups, genders = get_form_data()

if not countries:
    st.warning("Data untuk form prediksi tidak tersedia. Pastikan job 'silver_to_gold.py' sudah berjalan sukses.")
    st.stop()

st.subheader("1. Pilih Skenario Utama")
with st.form("prediction_form"):
    col1, col2 = st.columns(2)
    with col1:
        default_country_index = countries.index("United States") if "United States" in countries else 0
        country = st.selectbox("Negara", countries, index=default_country_index)
        disease_category = st.selectbox("Kategori Penyakit", disease_categories)
    
    with col2:
        year = st.number_input("Tahun", min_value=2000, max_value=2030, value=2023, step=1)
        age_group = st.selectbox("Kelompok Usia", age_groups)
        gender = st.selectbox("Gender", genders)

    submitted = st.form_submit_button("Dapatkan Prediksi")

if submitted:
    st.info(f"Mengambil data rata-rata untuk **{country}** untuk melengkapi fitur...")
    
    country_defaults_query = f"SELECT * FROM read_parquet('s3a://gold/country-health-summary/*.parquet') WHERE Country = '{country}'"
    df_defaults = query_duckdb(country_defaults_query)

    if df_defaults.empty:
        st.error(f"Tidak dapat menemukan data default untuk {country}. Tidak dapat melanjutkan prediksi.")
        st.stop()

    defaults = df_defaults.iloc[0]

    with st.expander("Lihat Fitur yang Diisi Otomatis"):
        st.write("Fitur berikut diisi otomatis berdasarkan data rata-rata negara atau nilai default sistem.")
        auto_filled_data = {
            "Prevalence Rate (%)": defaults.get('Avg_Prevalence_Rate_Percent', 5.0),
            "Population Affected": 100000,
            "Healthcare Access (%)": defaults.get('Avg_Healthcare_Access_Percent'),
            "Doctors per 1000": defaults.get('Avg_Doctors_per_1000'),
            "Hospital Beds per 1000": defaults.get('Avg_Hospital_Beds_per_1000'),
            "Avg Treatment Cost (USD)": defaults.get('Avg_Average_Treatment_Cost_USD', 1000),
            "Recovery Rate (%)": defaults.get('Avg_Recovery_Rate_Percent', 50.0),
            "Per Capita Income (USD)": defaults.get('Avg_Per_Capita_Income_USD'),
            "Education Index": defaults.get('Avg_Education_Index'),
            "Urbanization Rate (%)": defaults.get('Avg_Urbanization_Rate_Percent'),
            "Incidence Rate (%) (Default Sistem)": 1.0,
            "DALYs (Default Sistem)": 400.0
        }
        st.json({k: v for k, v in auto_filled_data.items() if v is not None})

    payload = {
        "Year": year,
        "Country": country,
        "Disease_Category": disease_category,
        "Age_Group": age_group,
        "Gender": gender,
        
        "Prevalence_Rate_Percent": float(defaults.get('Avg_Prevalence_Rate_Percent', 5.0)),
        "Population_Affected": 100000,
        
        "Healthcare_Access_Percent": float(defaults.get('Avg_Healthcare_Access_Percent', 0)),
        "Doctors_per_1000": float(defaults.get('Avg_Doctors_per_1000', 0)),
        "Hospital_Beds_per_1000": float(defaults.get('Avg_Hospital_Beds_per_1000', 0)),
        "Average_Treatment_Cost_USD": int(defaults.get('Avg_Average_Treatment_Cost_USD', 1000)),
        "Recovery_Rate_Percent": float(defaults.get('Avg_Recovery_Rate_Percent', 50.0)),
        "Per_Capita_Income_USD": int(defaults.get('Avg_Per_Capita_Income_USD', 0)),
        "Education_Index": float(defaults.get('Avg_Education_Index', 0)),
        "Urbanization_Rate_Percent": float(defaults.get('Avg_Urbanization_Rate_Percent', 0)),

        "Incidence_Rate_Percent": 1.0,
        "DALYs": 400.0,
    }

    try:
        response = requests.post(f"{FLASK_API_URL}/predict", json=payload)
        if response.status_code == 200:
            prediction = response.json()
            st.success(f"Prediksi Angka Kematian: **{prediction.get('mortality_rate_prediction'):.2f}%**")
        else:
            st.error(f"Gagal mendapatkan prediksi. Status: {response.status_code}, Pesan: {response.text}")
    except requests.exceptions.ConnectionError:
        st.error("Gagal terhubung ke layanan prediksi.")
