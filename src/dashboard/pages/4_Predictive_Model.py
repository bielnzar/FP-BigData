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

with st.expander("ℹ️ Panduan Penggunaan dan Interpretasi Model"):
    st.markdown("""
    Bagian ini menjelaskan cara menggunakan model prediksi dan memahami input yang diperlukan untuk mendapatkan hasil yang relevan.

    #### Cara Menggunakan
    1.  **Pilih Skenario Utama**: Mulailah dengan memilih `Negara`, `Kategori Penyakyt`, `Tahun`, `Kelompok Usia`, dan `Gender`. Ini adalah parameter dasar untuk prediksi Anda.
    2.  **Sesuaikan Detail Tambahan (Opsional)**: Anda dapat menyesuaikan `Tingkat Insidensi` dan `DALYs` untuk membuat skenario yang lebih spesifik. Jika Anda tidak yakin, biarkan nilai default.
    3.  **Fitur yang Diisi Otomatis**: Untuk menyederhanakan penggunaan, beberapa fitur sosio-ekonomi yang kompleks (seperti *Prevalence Rate*, *Average Treatment Cost*, dan *Recovery Rate*) akan diisi secara otomatis berdasarkan data rata-rata historis untuk negara yang Anda pilih. Anda dapat melihat nilai-nilai ini di hasil prediksi.
    4.  **Dapatkan Prediksi**: Klik tombol untuk melihat estimasi angka kematian berdasarkan input Anda.

    #### Panduan Input Tambahan
    -   **Tingkat Insidensi (%)**:
        -   **Definisi**: Persentase kasus **baru** dari suatu penyakit dalam populasi selama periode waktu tertentu (biasanya satu tahun).
        -   **Panduan**: Nilai yang lebih tinggi menunjukkan penyebaran penyakit yang lebih cepat. Untuk sebagian besar penyakit menular atau kronis, rentang nilai yang umum adalah antara **0.1% hingga 15%**. Nilai yang sangat tinggi (misalnya > 30%) mungkin tidak realistis kecuali dalam situasi wabah yang sangat parah dan terlokalisir.
        -   **Default**: `1.0%`

    -   **DALYs (Disability-Adjusted Life Years)**:
        -   **Definisi**: Ukuran beban penyakit keseluruhan, yang menggabungkan tahun hidup yang hilang karena kematian dini dan tahun hidup dengan disabilitas.
        -   **Panduan**: Nilai yang lebih tinggi menunjukkan dampak penyakit yang lebih parah terhadap kualitas hidup dan harapan hidup populasi. Rentang nilai sangat bervariasi:
            -   Penyakit ringan: **50 - 500**
            -   Penyakit sedang hingga berat: **500 - 5000**
            -   Penyakit sangat parah/mematikan: **> 5000**
        -   **Default**: `400.0`

    #### Interpretasi Hasil
    Hasil prediksi adalah **estimasi** yang dihasilkan oleh model *Random Forest Regressor* berdasarkan pola data historis. Angka ini bukanlah kepastian medis, melainkan alat bantu untuk analisis tren, perencanaan sumber daya kesehatan, dan pemahaman dampak berbagai faktor terhadap angka kematian.
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

    st.subheader("2. Sesuaikan Detail Tambahan (Opsional)")
    st.markdown("Fitur-fitur ini memiliki nilai default, tetapi dapat Anda sesuaikan untuk skenario yang lebih spesifik.")
    
    col3, col4 = st.columns(2)
    with col3:
        incidence_rate = st.number_input("Tingkat Insidensi (%)", min_value=0.0, max_value=100.0, value=1.0, step=0.1, format="%.1f")
    with col4:
        dalys = st.number_input("DALYs (Disability-Adjusted Life Years)", min_value=0.0, value=400.0, step=10.0, format="%.1f")

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
            "Avg Treatment Cost (USD)": defaults.get('Avg_Average_Treatment_Cost_USD', 1000),
            "Recovery Rate (%)": defaults.get('Avg_Recovery_Rate_Percent', 50.0),
            "Per Capita Income (USD)": defaults.get('Avg_Per_Capita_Income_USD', 0), # Ditambahkan
        }
        st.json({k: v for k, v in auto_filled_data.items() if v is not None})

    payload = {
        "Year": year,
        "Country": country,
        "Disease_Category": disease_category,
        "Age_Group": age_group,
        "Gender": gender,
        
        # Fitur yang diisi otomatis dari ringkasan negara
        "Prevalence_Rate_Percent": float(defaults.get('Avg_Prevalence_Rate_Percent', 5.0)),
        "Average_Treatment_Cost_USD": int(defaults.get('Avg_Average_Treatment_Cost_USD', 1000)),
        "Recovery_Rate_Percent": float(defaults.get('Avg_Recovery_Rate_Percent', 50.0)),
        "Per_Capita_Income_USD": int(defaults.get('Avg_Per_Capita_Income_USD', 0)), # Ditambahkan

        # Fitur dari input pengguna
        "Incidence_Rate_Percent": incidence_rate,
        "DALYs": dalys,
    }

    try:
        response = requests.post(f"{FLASK_API_URL}/predict", json=payload)
        if response.status_code == 200:
            prediction = response.json()
            st.subheader("📈 Hasil Prediksi")
            
            col1, col2, col3 = st.columns([1,2,1])
            with col2:
                st.metric(
                    label="Prediksi Angka Kematian",
                    value=f"{prediction.get('mortality_rate_prediction'):.2f}%"
                )
            
            st.info(f"Berdasarkan skenario yang diberikan, model memprediksi angka kematian sebesar **{prediction.get('mortality_rate_prediction'):.2f}%**.")
        else:
            st.error(f"Gagal mendapatkan prediksi. Status: {response.status_code}, Pesan: {response.text}")
    except requests.exceptions.ConnectionError:
        st.error("Gagal terhubung ke layanan prediksi.")
