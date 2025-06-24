import streamlit as st
import pandas as pd
from utils import query_trino
import altair as alt

st.set_page_config(page_title="Riset vs. Dampak", layout="wide")

st.title("Analisis Dampak Penyakit vs. Volume Riset")
st.markdown("""
Halaman ini memberikan wawasan unik dengan membandingkan dampak suatu penyakit (diukur dari total populasi terdampak) dengan volume riset yang dilakukan (diukur dari jumlah abstrak medis yang relevan). 
Gunakan scatter plot di bawah ini untuk mengidentifikasi:
- **Kuadran Kanan Atas**: Penyakit berdampak tinggi dengan riset yang banyak (fokus utama riset saat ini).
- **Kuadran Kanan Bawah**: Penyakit berdampak tinggi tetapi risetnya sedikit (potensi area riset yang terabaikan).
""")

query = "SELECT * FROM disease_impact_vs_research"
df_analysis = query_trino(query)

if not df_analysis.empty:
    numeric_cols = ['total_population_affected', 'avg_mortality_rate', 'abstract_count']
    for col in numeric_cols:
        df_analysis[col] = pd.to_numeric(df_analysis[col], errors='coerce')
    df_analysis.dropna(inplace=True)

    st.subheader("Data Dampak vs. Riset")
    st.dataframe(df_analysis)

    st.subheader("Scatter Plot: Dampak vs. Riset")
    
    chart = alt.Chart(df_analysis).mark_circle(size=100).encode(
        x=alt.X('total_population_affected:Q', title='Total Populasi Terdampak (Dampak)'),
        y=alt.Y('abstract_count:Q', title='Jumlah Abstrak Medis (Riset)'),
        color=alt.Color('disease_category:N', legend=alt.Legend(title="Kategori Penyakit")),
        tooltip=['disease_category', 'total_population_affected', 'abstract_count', 'avg_mortality_rate']
    ).interactive()

    st.altair_chart(chart, use_container_width=True)

else:
    st.info("Data analisis gabungan belum tersedia. Pastikan pipeline ETL telah berjalan.")
