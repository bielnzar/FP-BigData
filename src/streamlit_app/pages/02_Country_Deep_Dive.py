import streamlit as st
import pandas as pd
import plotly.express as px
from spark_minio_connector import load_delta_table_as_pandas, GOLD_ML_FEATURES_S3A_PATH # Impor baru
import logging

# Setup logger sederhana untuk halaman ini
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

st.set_page_config(page_title="Country Deep Dive", page_icon="🇰🇭", layout="wide")

st.markdown("# 🇰🇭 Analisis Mendalam per Negara")
st.sidebar.header("Filter Negara")

@st.cache_data(ttl=300) # Cache data selama 5 menit
def load_ml_features_data_from_minio(): # Nama fungsi diubah agar lebih jelas
    """Memuat data fitur ML dari tabel Gold Delta Lake di MinIO via Spark."""
    logger.info("Attempting to load ML features data from MinIO via Spark...")
    df = load_delta_table_as_pandas(GOLD_ML_FEATURES_S3A_PATH) # Menggunakan fungsi baru
    
    if not df.empty:
        logger.info(f"Successfully loaded {len(df)} rows for ML features.")
        # Konversi tipe data yang diperlukan (Delta Lake seharusnya menjaga tipe, tapi baik untuk verifikasi)
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
        
        numeric_feature_cols = [
            "feature_prevalence_rate", "feature_incidence_rate", "feature_population_affected",
            "feature_healthcare_access", "feature_doctors_per_1000", "feature_hospital_beds",
            "feature_per_capita_income", "feature_education_index", "feature_urbanization_rate",
            "label_mortality_rate"
        ]
        for col_name in numeric_feature_cols:
            if col_name in df.columns and df[col_name].dtype == 'object': # Cek jika masih object
                df[col_name] = pd.to_numeric(df[col_name], errors='coerce')
        return df
    logger.warning("Failed to load ML features data or data is empty.")
    return pd.DataFrame()

df_features = load_ml_features_data_from_minio() # Memanggil fungsi yang sudah diubah
# === AKHIR PERUBAHAN PADA FUNGSI LOAD DATA ===

if df_features.empty:
    st.warning("Tidak dapat memuat data fitur ML. Pastikan Spark dapat membaca dari MinIO dan tabel Gold 'ml_features_health_disparity' berisi data.")
else:
    all_countries_features = sorted(df_features["Country"].unique()) if "Country" in df_features.columns else []
    
    if not all_countries_features:
        st.warning("Tidak ada data negara yang tersedia di dataset fitur ML.")
    else:
        selected_country_dd = st.sidebar.selectbox(
            "Pilih Negara untuk Analisis Mendalam:",
            options=all_countries_features,
            index=0 # Default ke negara pertama dalam daftar
        )

        if selected_country_dd:
            st.subheader(f"Data Detail untuk: {selected_country_dd}")
            # Gunakan .copy() untuk menghindari SettingWithCopyWarning saat memfilter lebih lanjut
            df_country_specific = df_features[df_features["Country"] == selected_country_dd].copy() 
            
            if df_country_specific.empty:
                st.info(f"Tidak ada data detail untuk {selected_country_dd}.")
            else:
                st.markdown(f"Menampilkan {len(df_country_specific)} baris data untuk {selected_country_dd} (mencakup berbagai penyakit/tahun). Berikut adalah 5 baris pertama:")
                st.dataframe(df_country_specific.head())

                # Agregasi sederhana per tahun untuk negara yang dipilih
                # Pastikan kolom yang dibutuhkan ada sebelum agregasi
                required_cols_trend = ["Year", "label_mortality_rate", "feature_prevalence_rate", "feature_population_affected"]
                if all(col_name in df_country_specific.columns for col_name in required_cols_trend):
                    df_country_yearly = df_country_specific.groupby("Year").agg(
                        avg_mortality_rate=('label_mortality_rate', 'mean'),
                        avg_prevalence_rate=('feature_prevalence_rate', 'mean'),
                        total_population_affected=('feature_population_affected', 'sum')
                    ).reset_index()

                    st.markdown(f"### Tren Tahunan Indikator Kunci di {selected_country_dd}")
                    if not df_country_yearly.empty:
                        fig_country_trend = px.line(
                            df_country_yearly,
                            x="Year",
                            y=["avg_mortality_rate", "avg_prevalence_rate"],
                            title=f"Tren Rata-rata Mortalitas & Prevalensi di {selected_country_dd}",
                            labels={"Year": "Tahun", "value": "Rate/Nilai", "variable": "Indikator"}
                        )
                        st.plotly_chart(fig_country_trend, use_container_width=True)
                    else:
                        st.info(f"Tidak cukup data untuk menampilkan tren tahunan untuk {selected_country_dd}.")
                else:
                    st.info(f"Kolom yang dibutuhkan untuk tren tahunan ({', '.join(required_cols_trend)}) tidak ditemukan untuk {selected_country_dd}.")


                # Perbandingan penyakit di tahun terakhir yang tersedia untuk negara tersebut
                st.markdown(f"### Distribusi Beban Penyakit di {selected_country_dd} (Tahun Terakhir)")
                required_cols_pie = ["Year", "Disease_Name", "feature_population_affected"]
                if all(col_name in df_country_specific.columns for col_name in required_cols_pie) and not df_country_specific["Year"].isnull().all():
                    last_year_country = df_country_specific["Year"].max()
                    df_country_last_year = df_country_specific[df_country_specific["Year"] == last_year_country]

                    if not df_country_last_year.empty:
                        # Pastikan feature_population_affected adalah numerik dan tidak ada NaN untuk pie chart
                        df_pie_data = df_country_last_year.dropna(subset=['feature_population_affected'])
                        # Filter hanya nilai positif untuk pie chart
                        df_pie_data = df_pie_data[df_pie_data['feature_population_affected'] > 0] 

                        if not df_pie_data.empty:
                            fig_disease_dist = px.pie(
                                df_pie_data,
                                values='feature_population_affected',
                                names='Disease_Name',
                                title=f"Distribusi Populasi Terdampak per Penyakit di {selected_country_dd} ({last_year_country})",
                                hole=.3
                            )
                            fig_disease_dist.update_traces(textposition='inside', textinfo='percent+label')
                            st.plotly_chart(fig_disease_dist, use_container_width=True)
                        else:
                            st.info(f"Tidak ada data populasi terdampak yang valid untuk pie chart di {selected_country_dd} ({last_year_country}).")
                        
                        st.markdown(f"Data detail penyakit di {selected_country_dd} untuk tahun {last_year_country}:")
                        display_cols_detail = ["Disease_Name", "Disease_Category", "feature_prevalence_rate", 
                                        "feature_incidence_rate", "label_mortality_rate", "feature_population_affected"]
                        # Hanya tampilkan kolom yang ada di DataFrame
                        existing_display_cols = [col for col in display_cols_detail if col in df_country_last_year.columns]
                        st.dataframe(df_country_last_year[existing_display_cols])
                    else:
                        st.info(f"Tidak cukup data atau kolom yang dibutuhkan tidak ada untuk analisis penyakit di tahun terakhir ({last_year_country}) di {selected_country_dd}.")
                else:
                    st.info(f"Tidak ada data tahun yang valid atau kolom yang dibutuhkan ({', '.join(required_cols_pie)}) tidak ditemukan untuk analisis penyakit di {selected_country_dd}.")
        else:
            st.info("Silakan pilih negara dari sidebar untuk melihat analisis mendalam.")