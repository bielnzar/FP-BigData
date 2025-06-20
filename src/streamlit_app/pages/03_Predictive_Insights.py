# src/streamlit_app/pages/03_Predictive_Insights.py
import streamlit as st
import pandas as pd
# import requests # Untuk memanggil Flask API nantinya

st.set_page_config(page_title="Predictive Insights", page_icon="🔮", layout="wide")

st.markdown("# 🔮 Wawasan Prediktif (Under Construction)")
st.sidebar.header("Input untuk Prediksi")

st.warning(
    """
    Fitur ini sedang dalam pengembangan. 
    
    Di masa mendatang, Anda akan dapat memasukkan parameter sosio-ekonomi dan infrastruktur kesehatan 
    untuk mendapatkan prediksi indikator kesehatan kunci (seperti angka kematian atau DALYs) 
    menggunakan model Machine Learning yang telah dilatih.
    """
)

st.markdown("---")
st.subheader("Contoh Input yang Mungkin Diperlukan (Ilustrasi):")

# Contoh input (ini hanya placeholder)
col1, col2 = st.columns(2)
with col1:
    country_input = st.text_input("Nama Negara (untuk konteks):", "Contoh: Indonesia")
    year_input = st.number_input("Tahun Prediksi:", min_value=2024, max_value=2030, value=2025)
    education_index_input = st.slider("Indeks Pendidikan (0-1):", 0.0, 1.0, 0.65, 0.01)
    urbanization_rate_input = st.slider("Tingkat Urbanisasi (%):", 0.0, 100.0, 60.0, 0.5)

with col2:
    per_capita_income_input = st.number_input("Pendapatan per Kapita (USD):", min_value=500, value=5000)
    healthcare_access_input = st.slider("Akses Layanan Kesehatan (%):", 0.0, 100.0, 70.0, 0.5)
    doctors_per_1000_input = st.slider("Dokter per 1000 Penduduk:", 0.0, 10.0, 2.5, 0.1)
    # Tambahkan input lain yang relevan dengan fitur model Anda

if st.button("Dapatkan Prediksi (Fitur Belum Aktif)"):
    st.info("Memproses... (Fitur prediksi belum diimplementasikan)")
    
    # Nantinya, di sini akan ada logika untuk:
    # 1. Mengumpulkan input menjadi format yang sesuai untuk Flask API
    # input_data = {
    #     "feature_education_index": education_index_input,
    #     "feature_urbanization_rate": urbanization_rate_input,
    #     "feature_per_capita_income": per_capita_income_input,
    #     "feature_healthcare_access": healthcare_access_input,
    #     "feature_doctors_per_1000": doctors_per_1000_input,
    #     # ... fitur lainnya ...
    # }
    # 2. Memanggil Flask API
    # FLASK_API_URL = "http://flask_api_service_name:port/predict" # Ganti dengan URL API Anda
    # try:
    #     response = requests.post(FLASK_API_URL, json=input_data)
    #     response.raise_for_status() # Akan error jika status code 4xx atau 5xx
    #     prediction_result = response.json()
    #     st.subheader("Hasil Prediksi:")
    #     # Tampilkan hasil prediksi, misalnya:
    #     # st.metric(label="Prediksi Angka Kematian (%)", value=f"{prediction_result.get('predicted_mortality_rate', 'N/A'):.2f}")
    #     st.json(prediction_result)
    # except requests.exceptions.RequestException as e:
    #     st.error(f"Gagal menghubungi API prediksi: {e}")
    # except Exception as e:
    #     st.error(f"Terjadi kesalahan saat memproses prediksi: {e}")

st.markdown("---")
st.caption("Model prediksi akan diintegrasikan di sini setelah pelatihan dan deployment API.")