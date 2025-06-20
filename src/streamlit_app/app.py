import streamlit as st

st.set_page_config(
    page_title="Global Health Disparity Analytics",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("🌍 Platform Analitik Disparitas Kesehatan Global")
st.markdown("---")

st.sidebar.success("Pilih halaman di atas untuk navigasi.")

st.markdown(
    """
    Selamat datang di Platform Analitik Prediktif untuk Pengentasan Disparitas Kesehatan Global.
    
    Platform ini bertujuan untuk memberikan wawasan mendalam mengenai bagaimana faktor-faktor sosio-ekonomi 
    dan infrastruktur kesehatan secara kolektif memengaruhi beban penyakit yang berbeda di berbagai negara.

    **Gunakan sidebar di sebelah kiri untuk menavigasi ke berbagai analisis:**
    - **Global Overview:** Lihat tren dan perbandingan indikator kesehatan secara global.
    - **Country Deep Dive:** Analisis lebih mendalam untuk negara tertentu.
    - **Predictive Insights:** (Under Construction) Dapatkan prediksi indikator kesehatan kunci.

    Data yang digunakan dalam platform ini diproses melalui pipeline Big Data modern yang melibatkan Kafka, Spark,
    MinIO (Data Lake), Delta Lake, dan Trino.
    """
)

st.subheader("Tentang Proyek Ini")
st.markdown(
    """
    Proyek ini merupakan bagian dari mata kuliah Big Data dan Data Lakehouse. 
    Fokus utama adalah merancang dan mengimplementasikan arsitektur end-to-end 
    untuk pengolahan dan analisis data kesehatan global.
    """
)

st.markdown("---")
st.caption("Kelompok 6 - Big Data & Data Lakehouse (A)")

# Anda bisa menambahkan gambar arsitektur di sini jika mau
# from PIL import Image
# image = Image.open('path/to/your/architecture_diagram.png') # Simpan gambar di folder yang sama atau subfolder
# st.image(image, caption='Diagram Arsitektur Solusi')