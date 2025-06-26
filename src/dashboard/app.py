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
    Selamat datang di **Platform Analitik Kesehatan Global**.
    
    Platform ini dirancang untuk menjelajahi, menganalisis, dan memprediksi tren kesehatan di seluruh dunia. Kami memanfaatkan kekuatan Big Data untuk mengubah data mentah menjadi wawasan yang dapat ditindaklanjuti.
    
    **👈 Pilih halaman dari sidebar** untuk mulai menjelajahi berbagai analisis:
    - **Ringkasan per Negara**: Dapatkan potret kesehatan suatu negara dalam sekejap.
    - **Tren Tahunan**: Lihat bagaimana metrik kesehatan global berubah dari waktu ke waktu.
    - **Analisis Abstrak Medis**: Temukan fokus penelitian medis terkini melalui analisis teks.
    - **Model Prediktif**: Gunakan model Machine Learning kami untuk memprediksi angka kematian berdasarkan berbagai faktor.
    
    ---
    
    ### Arsitektur & Teknologi
    Platform ini dibangun di atas tumpukan teknologi modern yang tangguh dan skalabel:
    """
)

col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("📊 Ingest & Storage")
    st.markdown("- **Apache Kafka**: Aliran data real-time.")
    st.markdown("- **MinIO**: Data Lake berbasis Object Storage.")

with col2:
    st.subheader("⚙️ Processing & Analytics")
    st.markdown("- **Apache Spark**: ETL dan pemrosesan data skala besar.")
    st.markdown("- **DuckDB**: Mesin kueri analitik super cepat.")

with col3:
    st.subheader("🚀 Serving & Visualization")
    st.markdown("- **Flask**: API untuk menyajikan model ML.")
    st.markdown("- **Streamlit**: Dashboard interaktif yang sedang Anda lihat.")
