import streamlit as st
import pandas as pd
from utils import query_duckdb
import altair as alt

st.set_page_config(page_title="Tren Tahunan Global", layout="wide")
st.title("📈 Tren Penyakit Tahunan Global")
st.markdown("Analisis tren global untuk berbagai kategori penyakit dari tahun ke tahun.")

query = "SELECT * FROM read_parquet('s3a://gold/yearly-disease-category-summary/*.parquet') ORDER BY Year, Disease_Category"
df_trends = query_duckdb(query)

if not df_trends.empty:
    st.dataframe(df_trends)

    with st.expander("💡 Penjelasan Kolom pada Tabel Tren"):
        st.markdown("""
        Tabel di atas menampilkan agregasi data kesehatan global yang dikelompokkan berdasarkan tahun dan kategori penyakit.
        
        - **Year**: Tahun pencatatan data.
        - **Disease_Category**: Kategori umum dari penyakit yang dianalisis.
        - **Global_Avg_Mortality_Rate_Percent**: Rata-rata angka kematian global (dalam persen) untuk kategori penyakit tersebut pada tahun yang bersangkutan.
        - **Global_Avg_DALYs**: Rata-rata global *Disability-Adjusted Life Years* (DALYs), sebuah ukuran beban penyakit.
        - **Global_Total_Population_Affected**: Jumlah total populasi yang terdampak oleh kategori penyakit ini di seluruh dunia pada tahun tersebut.
        - **Num_Countries_Reported**: Jumlah negara yang melaporkan data untuk kategori penyakit ini pada tahun tersebut.
        """)

    metric_options = {
        'Global_Avg_Mortality_Rate_Percent': 'Rata-rata Global Angka Kematian (%)',
        'Global_Avg_DALYs': 'Rata-rata Global DALYs',
        'Global_Total_Population_Affected': 'Total Global Populasi Terdampak'
    }
    
    selected_metric_col = st.selectbox(
        "Pilih Metrik untuk Divisualisasikan:",
        options=list(metric_options.keys()),
        format_func=lambda x: metric_options[x]
    )

    chart = alt.Chart(df_trends).mark_area(opacity=0.7).encode(
        x=alt.X("Year:O", title="Tahun"),
        y=alt.Y(f"{selected_metric_col}:Q", title=metric_options[selected_metric_col]),
        color=alt.Color("Disease_Category:N", title="Kategori Penyakit"),
        tooltip=['Year', 'Disease_Category', selected_metric_col]
    ).properties(
        title=f"Tren Global untuk {metric_options[selected_metric_col]}"
    ).interactive()
    
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("Data tren tahunan tidak ditemukan. Pastikan job 'silver_to_gold.py' sudah berjalan sukses.")
