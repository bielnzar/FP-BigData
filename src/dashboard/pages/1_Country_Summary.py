import streamlit as st
import pandas as pd
from utils import query_trino

st.set_page_config(page_title="Ringkasan per Negara", layout="wide")

st.title("Ringkasan Kesehatan per Negara")
st.markdown("""
Halaman ini menampilkan ringkasan metrik kesehatan yang diagregasi untuk setiap negara. 
Data ini memberikan gambaran umum tentang kondisi kesehatan, akses layanan, dan faktor sosio-ekonomi di berbagai negara. 
Gunakan data ini untuk membandingkan performa kesehatan antar negara secara sekilas.
""")

query = "SELECT * FROM country_health_summary"
df_summary = query_trino(query)

if not df_summary.empty:
    st.dataframe(df_summary)

    st.subheader("Perbandingan Metrik Kesehatan Antar Negara")
    
    numeric_cols = [
        'avg_mortality_rate_percent', 'avg_dalys', 'avg_prevalence_rate_percent',
        'total_population_affected_overall', 'avg_healthcare_access_percent',
        'avg_doctors_per_1000', 'avg_per_capita_income_usd'
    ]
    for col in numeric_cols:
        if col in df_summary.columns:
            df_summary[col] = pd.to_numeric(df_summary[col], errors='coerce')

    metric = st.selectbox(
        "Pilih metrik untuk divisualisasikan:",
        options=numeric_cols,
        format_func=lambda x: x.replace('_', ' ').replace('avg', 'Rata-rata').replace('percent', '%').title()
    )

    countries = st.multiselect(
        "Pilih negara untuk dibandingkan (pilih beberapa):",
        options=sorted(df_summary['country'].unique()),
        default=df_summary['country'].head(5).tolist()
    )

    if countries:
        df_filtered = df_summary[df_summary['country'].isin(countries)]
        st.bar_chart(df_filtered.set_index('country')[metric])
    else:
        st.warning("Silakan pilih setidaknya satu negara.")

else:
    st.info("Data ringkasan negara belum tersedia. Pastikan pipeline ETL telah berjalan.")
