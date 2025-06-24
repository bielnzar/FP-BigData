import streamlit as st

st.set_page_config(
    page_title="Global Health Analytics",
    page_icon="🌍",
    layout="wide"
)

st.title("🌍 Platform Analitik Kesehatan Global")

st.sidebar.success("Pilih halaman analisis di atas.")

st.markdown(
    """
    Selamat datang di Platform Analitik Kesehatan Global.
    
    Platform ini dibangun untuk menganalisis disparitas kesehatan di seluruh dunia dengan memanfaatkan dataset terstruktur dan tidak terstruktur.
    
    **👈 Pilih halaman dari sidebar** untuk mulai menjelajahi berbagai wawasan:
    - **Ringkasan per Negara**: Lihat metrik kesehatan utama yang diagregasi per negara.
    - **Tren Tahunan**: Analisis tren penyakit secara global dari tahun ke tahun.
    - **Riset vs. Dampak**: Bandingkan volume riset dengan dampak penyakit.
    - **Model Prediktif**: Gunakan model Machine Learning untuk memprediksi angka kematian.
    
    ### Teknologi yang Digunakan
    Platform ini didukung oleh arsitektur data modern yang mencakup:
    - **Apache Kafka**: Untuk ingest data secara real-time.
    - **Apache Spark**: Untuk pemrosesan data ETL dan pelatihan model.
    - **MinIO**: Sebagai object storage untuk Data Lake.
    - **Trino**: Sebagai mesin kueri cepat untuk analitik interaktif.
    - **Flask**: Untuk menyajikan model Machine Learning sebagai API.
    - **Streamlit**: Untuk membangun dashboard interaktif ini.
    """
)
