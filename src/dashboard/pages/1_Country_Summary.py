import streamlit as st
import pandas as pd
from utils import query_duckdb
import altair as alt
from urllib.parse import quote

# Konfigurasi MinIO untuk bendera
# PENTING: Jika Anda menjalankan Streamlit dari dalam container Docker atau dari mesin yang berbeda
# dengan MinIO, ganti "localhost" dengan alamat IP atau hostname dari mesin yang menjalankan MinIO
# yang dapat diakses dari browser Anda.
MINIO_ENDPOINT = "localhost:9000"
MINIO_BUCKET_NAME = "flags"

st.set_page_config(page_title="Ringkasan per Negara", layout="wide")
st.title("📊 Ringkasan Kesehatan per Negara")
st.markdown("Pilih sebuah negara untuk melihat ringkasan statistik kesehatan dan tren penyakit dari waktu ke waktu.")

@st.cache_data(ttl=3600)
def load_countries():
    """Memuat daftar negara dari data gold menggunakan DuckDB."""
    query = "SELECT DISTINCT Country FROM read_parquet('s3a://gold/country-health-summary/*.parquet') ORDER BY Country"
    df = query_duckdb(query)
    return df['Country'].tolist() if not df.empty else []

countries = load_countries()

if not countries:
    st.warning("Data negara tidak ditemukan. Pastikan job 'silver_to_gold.py' sudah berjalan sukses.")
    st.stop()

default_country_index = countries.index("United States") if "United States" in countries else 0
selected_country = st.selectbox("Pilih Negara:", countries, index=default_country_index)

col1, col2 = st.columns([0.9, 0.1])
with col1:
    st.header(f"Ringkasan Umum untuk {selected_country}")
with col2:
    # URL encode nama negara untuk URL yang aman
    safe_country_name = quote(selected_country)
    flag_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET_NAME}/{safe_country_name}.png"
    st.image(flag_url, width=80)


summary_query = f"SELECT * FROM read_parquet('s3a://gold/country-health-summary/*.parquet') WHERE Country = '{selected_country}'"
df_summary = query_duckdb(summary_query)

if not df_summary.empty:
    st.dataframe(df_summary.T.rename(columns={0: 'Value'}))

    with st.expander("💡 Penjelasan Kolom pada Tabel Ringkasan"):
        st.markdown("""
        Tabel di atas menampilkan nilai rata-rata atau total dari berbagai metrik kesehatan dan sosio-ekonomi untuk negara yang dipilih selama periode data yang tersedia.
        
        - **Avg_Mortality_Rate_Percent**: Rata-rata persentase kematian dari total kasus penyakit yang dilaporkan.
        - **Avg_DALYs**: Rata-rata *Disability-Adjusted Life Years*, ukuran beban penyakit yang menggabungkan tahun hidup yang hilang karena kematian dini dan tahun hidup dengan disabilitas.
        - **Avg_Prevalence_Rate_Percent**: Rata-rata persentase populasi yang memiliki penyakit pada satu titik waktu tertentu.
        - **Avg_Incidence_Rate_Percent**: Rata-rata persentase kasus baru dari suatu penyakit dalam periode waktu tertentu.
        - **Total_Population_Affected_Overall**: Jumlah total kumulatif orang yang terdampak oleh berbagai penyakit.
        - **Avg_Healthcare_Access_Percent**: Rata-rata persentase populasi yang memiliki akses ke layanan kesehatan.
        - **Avg_Doctors_per_1000**: Rata-rata jumlah dokter per 1000 penduduk.
        - **Avg_Hospital_Beds_per_1000**: Rata-rata jumlah tempat tidur rumah sakit per 1000 penduduk.
        - **Avg_Average_Treatment_Cost_USD**: Rata-rata biaya perawatan penyakit dalam Dolar AS.
        - **Avg_Recovery_Rate_Percent**: Rata-rata persentase pasien yang sembuh dari penyakit.
        - **Avg_Per_Capita_Income_USD**: Rata-rata pendapatan per kapita, sebagai indikator ekonomi.
        - **Avg_Education_Index**: Rata-rata indeks pendidikan, mengukur tingkat pencapaian pendidikan.
        - **Avg_Urbanization_Rate_Percent**: Rata-rata persentase populasi yang tinggal di daerah perkotaan.
        - **Num_Years_Data**: Jumlah tahun unik di mana data tersedia untuk negara ini.
        - **First_Year_Data**: Tahun data paling awal yang tercatat.
        - **Last_Year_Data**: Tahun data paling akhir yang tercatat.
        """)
else:
    st.info("Tidak ada data ringkasan untuk negara yang dipilih.")

st.header(f"Detail Tren Penyakit per Tahun untuk {selected_country}")

details_query = f"""
SELECT 
    Year, 
    Disease_Category, 
    avg(Mortality_Rate_Percent) as avg_mortality
FROM read_parquet('s3a://gold/country-disease-yearly-details/*.parquet')
WHERE Country = '{selected_country}'
GROUP BY Year, Disease_Category
ORDER BY Year, Disease_Category
"""
df_details = query_duckdb(details_query)

if not df_details.empty:
    chart = alt.Chart(df_details).mark_line(point=True).encode(
        x=alt.X('Year:O', title='Tahun'),
        y=alt.Y('avg_mortality:Q', title='Rata-rata Angka Kematian (%)'),
        color=alt.Color('Disease_Category:N', title='Kategori Penyakit'),
        tooltip=['Year', 'Disease_Category', 'avg_mortality']
    ).properties(
        title=f"Tren Angka Kematian Rata-rata di {selected_country}"
    ).interactive()
    st.altair_chart(chart, use_container_width=True)
else:
    st.info("Tidak ada data detail untuk negara yang dipilih.")
