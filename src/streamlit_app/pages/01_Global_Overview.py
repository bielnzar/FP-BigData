# src/streamlit_app/pages/01_Global_Overview.py
import streamlit as st
import pandas as pd
import plotly.express as px
from api_connector import fetch_data_from_api # Impor dari file connector API baru
import logging

# Setup logger sederhana untuk halaman ini
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

st.set_page_config(page_title="Global Overview", page_icon="📊", layout="wide")

st.markdown("# 📊 Tinjauan Global Indikator Kesehatan")
st.sidebar.header("Filter Global Overview")

@st.cache_data(ttl=300)
def load_country_summary_data():
    """Memuat data ringkasan dari tabel Gold Delta Lake di MinIO."""
    logger.info("Attempting to load country summary data from MinIO via Spark...")
    df = load_delta_table_as_pandas(GOLD_COUNTRY_YEARLY_SUMMARY_S3A_PATH) 
    if not df.empty:
        logger.info(f"Successfully loaded {len(df)} rows for country summary.")
        # Konversi tipe data (jika belum benar dari Spark, tapi Delta seharusnya menjaga tipe)
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce').astype('Int64')
        # Anda mungkin tidak perlu casting tipe manual sebanyak ini jika Spark membaca skema Delta dengan benar
        return df
    logger.warning("Failed to load country summary data or data is empty.")
    return pd.DataFrame()

df_summary = load_country_summary_data()

if df_summary.empty:
    st.warning("Tidak dapat memuat data ringkasan. Pastikan Flask API berjalan dan endpoint '/api/gold_data/country_summary' berfungsi serta tabel Gold 'country_yearly_health_summary' berisi data.")
else:
    st.markdown("### Data Ringkasan Kesehatan Global per Negara per Tahun")
    st.caption(f"Menampilkan {len(df_summary)} total baris data. Berikut adalah 5 baris pertama:")
    st.dataframe(df_summary.head())

    # --- Filter ---
    try:
        all_countries = sorted(df_summary["Country"].unique()) if "Country" in df_summary.columns else []
        default_countries = all_countries[:5] if len(all_countries) >= 5 else all_countries
        
        selected_countries = st.sidebar.multiselect(
            "Pilih Negara:",
            options=all_countries,
            default=default_countries
        )

        if "Year" in df_summary.columns and not df_summary["Year"].isnull().all():
            min_year, max_year = int(df_summary["Year"].min()), int(df_summary["Year"].max())
            selected_year_range = st.sidebar.slider(
                "Pilih Rentang Tahun:",
                min_value=min_year,
                max_value=max_year,
                value=(min_year, max_year)
            )
            
            # Filter DataFrame berdasarkan pilihan
            filtered_df = df_summary[
                (df_summary["Country"].isin(selected_countries)) &
                (df_summary["Year"] >= selected_year_range[0]) &
                (df_summary["Year"] <= selected_year_range[1])
            ]
        else:
            st.sidebar.warning("Kolom 'Year' tidak tersedia atau kosong untuk filtering.")
            filtered_df = df_summary[df_summary["Country"].isin(selected_countries)] if "Country" in df_summary.columns else pd.DataFrame()


        if filtered_df.empty and (selected_countries or "Year" in df_summary.columns):
            st.warning("Tidak ada data untuk filter yang dipilih.")
        elif not filtered_df.empty :
            st.markdown(f"Menampilkan data untuk **{', '.join(selected_countries)}** dari tahun **{selected_year_range[0] if 'Year' in df_summary.columns else 'N/A'}** hingga **{selected_year_range[1] if 'Year' in df_summary.columns else 'N/A'}**.")
            st.dataframe(filtered_df.reset_index(drop=True))

            # --- Visualisasi ---
            st.markdown("### Visualisasi Tren Indikator")

            numeric_columns = [col for col in filtered_df.columns if pd.api.types.is_numeric_dtype(filtered_df[col]) and col not in ["Year", "count_records_for_Population_Affected", "count_records_for_DALYs"]]
            
            if not numeric_columns:
                st.info("Tidak ada kolom numerik yang tersedia untuk visualisasi.")
            else:
                default_y_index = numeric_columns.index("avg_Mortality_Rate_Percent") if "avg_Mortality_Rate_Percent" in numeric_columns else 0
                selected_indicator_y = st.selectbox(
                    "Pilih Indikator (Sumbu Y):",
                    options=numeric_columns,
                    index=default_y_index
                )

                if selected_indicator_y and "Year" in filtered_df.columns:
                    fig_trend = px.line(
                        filtered_df,
                        x="Year",
                        y=selected_indicator_y,
                        color="Country",
                        title=f"Tren {selected_indicator_y.replace('_', ' ').title()} per Tahun",
                        labels={"Year": "Tahun", selected_indicator_y: selected_indicator_y.replace('_', ' ').title(), "Country": "Negara"}
                    )
                    fig_trend.update_layout(legend_title_text='Negara')
                    st.plotly_chart(fig_trend, use_container_width=True)

                st.markdown("### Perbandingan Antar Negara (Tahun Terakhir dalam Filter)")
                
                last_year_in_range = selected_year_range[1] if "Year" in df_summary.columns else None
                if last_year_in_range:
                    df_last_year = filtered_df[filtered_df["Year"] == last_year_in_range]

                    if not df_last_year.empty:
                        default_bar_index = numeric_columns.index("avg_Per_Capita_Income_USD") if "avg_Per_Capita_Income_USD" in numeric_columns else 0
                        selected_indicator_bar = st.selectbox(
                            "Pilih Indikator untuk Perbandingan (Bar Chart):",
                            options=numeric_columns,
                            index=default_bar_index,
                            key="bar_indicator"
                        )

                        if selected_indicator_bar:
                            fig_bar = px.bar(
                                df_last_year.sort_values(by=selected_indicator_bar, ascending=False),
                                x="Country",
                                y=selected_indicator_bar,
                                color="Country",
                                title=f"Perbandingan {selected_indicator_bar.replace('_', ' ').title()} di Tahun {last_year_in_range}",
                                labels={"Country": "Negara", selected_indicator_bar: selected_indicator_bar.replace('_', ' ').title()}
                            )
                            fig_bar.update_layout(showlegend=False)
                            st.plotly_chart(fig_bar, use_container_width=True)
                        
                        st.markdown("### Analisis Korelasi Sederhana (Tahun Terakhir)")
                        col1_scatter, col2_scatter = st.columns(2)
                        default_x_scatter = numeric_columns.index("avg_Education_Index") if "avg_Education_Index" in numeric_columns else (1 if len(numeric_columns) > 1 else 0)
                        default_y_scatter = numeric_columns.index("avg_Per_Capita_Income_USD") if "avg_Per_Capita_Income_USD" in numeric_columns else (2 if len(numeric_columns) > 2 else 0)

                        with col1_scatter:
                            x_axis_scatter = st.selectbox("Pilih Indikator Sumbu X (Scatter):", numeric_columns, index=default_x_scatter)
                        with col2_scatter:
                            y_axis_scatter = st.selectbox("Pilih Indikator Sumbu Y (Scatter):", numeric_columns, index=default_y_scatter)
                        
                        if x_axis_scatter and y_axis_scatter:
                            size_col = "sum_Population_Affected" if "sum_Population_Affected" in df_last_year.columns else None
                            fig_scatter = px.scatter(
                                df_last_year,
                                x=x_axis_scatter,
                                y=y_axis_scatter,
                                color="Country",
                                size=size_col,
                                hover_name="Country",
                                title=f"Korelasi antara {x_axis_scatter.replace('_',' ')} dan {y_axis_scatter.replace('_',' ')} (Tahun {last_year_in_range})",
                                labels={x_axis_scatter: x_axis_scatter.replace('_',' ').title(), y_axis_scatter: y_axis_scatter.replace('_',' ').title()}
                            )
                            st.plotly_chart(fig_scatter, use_container_width=True)
                    else:
                        st.info(f"Tidak ada data untuk tahun {last_year_in_range} dalam filter yang dipilih.")
                else:
                    st.info("Tidak dapat menentukan tahun terakhir untuk perbandingan.")
        elif selected_countries : # Jika ada negara terpilih tapi filtered_df kosong karena masalah tahun
             st.warning("Tidak ada data untuk filter yang dipilih (mungkin karena rentang tahun).")

    except Exception as e:
        logger.error(f"Error rendering Global Overview page: {e}", exc_info=True)
        st.error(f"Terjadi kesalahan saat menampilkan halaman: {e}")