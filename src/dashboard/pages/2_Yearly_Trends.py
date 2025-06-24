import streamlit as st
import pandas as pd
from utils import query_trino

st.set_page_config(page_title="Tren Tahunan", layout="wide")

st.title("Tren Penyakit Global Tahunan")
st.markdown("""
Halaman ini menyajikan tren penyakit yang diagregasi per tahun dan kategori penyakit secara global. 
Analisis ini membantu dalam memahami bagaimana beban penyakit tertentu berubah dari waktu ke waktu di seluruh dunia.
""")

query = "SELECT * FROM yearly_disease_category_summary ORDER BY year"
df_trends = query_trino(query)

if not df_trends.empty:
    numeric_cols = ['global_avg_mortality_rate_percent', 'global_avg_dalys', 'global_total_population_affected']
    for col in numeric_cols:
        df_trends[col] = pd.to_numeric(df_trends[col], errors='coerce')
    df_trends['year'] = pd.to_numeric(df_trends['year'], errors='coerce')

    st.subheader("Data Tren Tahunan per Kategori Penyakit")
    st.dataframe(df_trends)

    st.subheader("Visualisasi Tren Angka Kematian Rata-rata Global")
    
    categories = st.multiselect(
        "Pilih Kategori Penyakit:",
        options=sorted(df_trends['disease_category'].unique()),
        default=list(df_trends['disease_category'].unique()[:3])
    )

    if categories:
        df_filtered = df_trends[df_trends['disease_category'].isin(categories)]
        df_pivot = df_filtered.pivot(index='year', columns='disease_category', values='global_avg_mortality_rate_percent')
        st.line_chart(df_pivot)
    else:
        st.warning("Silakan pilih setidaknya satu kategori penyakit.")

else:
    st.info("Data tren tahunan belum tersedia. Pastikan pipeline ETL telah berjalan.")
