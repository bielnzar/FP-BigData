import streamlit as st
import requests
from utils import FLASK_API_URL

st.set_page_config(page_title="Model Prediktif", layout="wide")

st.title("🔮 Prediksi Angka Kematian")
st.markdown("""
Halaman ini memungkinkan Anda untuk berinteraksi langsung dengan model Machine Learning yang telah dilatih. 
Masukkan berbagai faktor sosio-ekonomi dan kesehatan di bawah ini untuk mendapatkan prediksi angka kematian. 
Ini mensimulasikan bagaimana seorang analis dapat menggunakan model untuk skenario "what-if".
""")

try:
    health_check = requests.get(f"{FLASK_API_URL}/health")
    if health_check.status_code == 200 and health_check.json().get('status') == 'OK':
        st.success("Layanan Prediksi (Flask API) terhubung dan model berhasil dimuat.")
    else:
        st.error(f"Layanan Prediksi terhubung, tetapi model GAGAL dimuat. Pesan: {health_check.json().get('status')}")
        st.stop()
except requests.exceptions.ConnectionError:
    st.error("Gagal terhubung ke Layanan Prediksi (Flask API). Pastikan layanan 'flask-api' berjalan.")
    st.stop()

with st.form("prediction_form"):
    st.write("Masukkan fitur untuk prediksi:")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        year = st.number_input("Year", value=2022, step=1, help="Tahun data.")
        prevalence_rate = st.number_input("Prevalence Rate (%)", value=5.0)
        incidence_rate = st.number_input("Incidence Rate (%)", value=1.0)
        dalys = st.number_input("DALYs", value=400.0)

    with col2:
        population_affected = st.number_input("Population Affected", value=100000)
        healthcare_access = st.number_input("Healthcare Access (%)", value=80.0)
        doctors_per_1000 = st.number_input("Doctors per 1000", value=2.5)
        per_capita_income = st.number_input("Per Capita Income (USD)", value=30000.0)

    with col3:
        hospital_beds = st.number_input("Hospital Beds per 1000", value=3.0)
        avg_treatment_cost = st.number_input("Avg Treatment Cost (USD)", value=1500.0)
        recovery_rate = st.number_input("Recovery Rate (%)", value=90.0)
        education_index = st.number_input("Education Index", value=0.85)
        urbanization_rate = st.number_input("Urbanization Rate (%)", value=70.0)

    st.markdown("---")
    country = st.selectbox("Country", ["USA", "Germany", "Japan", "India", "Brazil"])
    disease_category = st.selectbox("Disease Category", ["Cardiovascular Diseases", "Infectious Diseases", "Cancers", "Respiratory Diseases"])
    age_group = st.selectbox("Age Group", ["0-19", "20-39", "40-59", "60-79", "80+"])
    gender = st.selectbox("Gender", ["Male", "Female"])

    submitted = st.form_submit_button("Dapatkan Prediksi")

if submitted:
    payload = {
        "Year": year, "Prevalence_Rate_Percent": prevalence_rate, "Incidence_Rate_Percent": incidence_rate,
        "Population_Affected": int(population_affected), "Healthcare_Access_Percent": healthcare_access,
        "Doctors_per_1000": doctors_per_1000, "Hospital_Beds_per_1000": hospital_beds,
        "Average_Treatment_Cost_USD": int(avg_treatment_cost), "Recovery_Rate_Percent": recovery_rate,
        "DALYs": dalys, "Per_Capita_Income_USD": int(per_capita_income), "Education_Index": education_index,
        "Urbanization_Rate_Percent": urbanization_rate, "Country": country, "Disease_Category": disease_category,
        "Age_Group": age_group, "Gender": gender
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
