import streamlit as st
import pandas as pd
from utils import query_duckdb
import altair as alt
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import nltk
from collections import Counter
import re

st.set_page_config(page_title="Analisis Abstrak Medis", layout="wide")

st.title("🔬 Analisis Teks Abstrak Medis")
st.markdown("""
Halaman ini memungkinkan Anda untuk mengeksplorasi kata kunci yang paling sering muncul dalam abstrak penelitian medis untuk kategori penyakit tertentu. 
Pilih kategori penyakit untuk menghasilkan *word cloud* dan melihat istilah yang paling dominan.
""")

# Inisialisasi stopwords
try:
    stop_words = set(nltk.corpus.stopwords.words('english'))
except LookupError:
    st.info("Mengunduh data NLTK (stopwords)...")
    nltk.download('stopwords')
    stop_words = set(nltk.corpus.stopwords.words('english'))

@st.cache_data(ttl=3600)
def load_labels():
    """Memuat daftar kategori penyakit (label) dari data silver."""
    query = "SELECT DISTINCT label FROM read_parquet('s3a://silver/medical-abstracts/*.parquet') ORDER BY label"
    df = query_duckdb(query)
    return df['label'].tolist() if not df.empty else []

@st.cache_data(ttl=3600)
def load_texts_for_label(label):
    """Memuat semua teks abstrak untuk label yang dipilih."""
    query = f"SELECT text FROM read_parquet('s3a://silver/medical-abstracts/*.parquet') WHERE label = '{label}'"
    df = query_duckdb(query)
    return df['text'].tolist() if not df.empty else []

labels = load_labels()

if not labels:
    st.warning("Data abstrak medis tidak ditemukan. Pastikan job 'bronze_to_silver.py' sudah berjalan sukses.")
    st.stop()

selected_label = st.selectbox("Pilih Kategori Penyakit untuk Dianalisis:", labels)

if selected_label:
    texts = load_texts_for_label(selected_label)
    
    if not texts:
        st.info(f"Tidak ada data teks untuk kategori '{selected_label}'.")
    else:
        st.metric(
            label="Jumlah Abstrak Dianalisis",
            value=f"{len(texts):,}"
        )
        st.markdown("---")

        st.subheader(f"Analisis Kata Kunci untuk: {selected_label.title()}")
        
        full_text = " ".join(texts)
        words = nltk.word_tokenize(full_text.lower())
        words_cleaned = [word for word in words if word.isalpha() and word not in stop_words and len(word) > 2]
        word_counts = Counter(words_cleaned)
        
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### Word Cloud")
            try:
                wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(word_counts)
                fig, ax = plt.subplots()
                ax.imshow(wordcloud, interpolation='bilinear')
                ax.axis('off')
                st.pyplot(fig)
            except Exception as e:
                st.error(f"Gagal membuat word cloud: {e}")

        with col2:
            st.markdown("#### Top 20 Kata Paling Sering Muncul")
            df_word_counts = pd.DataFrame(word_counts.most_common(20), columns=['Kata', 'Frekuensi'])
            
            chart = alt.Chart(df_word_counts).mark_bar().encode(
                x=alt.X('Frekuensi:Q'),
                y=alt.Y('Kata:N', sort='-x'),
                tooltip=['Kata', 'Frekuensi']
            ).interactive()
            
            st.altair_chart(chart, use_container_width=True)

        st.markdown("---")
        st.subheader(f"Contoh Teks Abstrak")
        st.markdown("Berikut adalah beberapa abstrak yang relevan dengan kategori yang dipilih, digabungkan menjadi satu paragraf besar untuk memberikan gambaran:")

        combined_texts = " ".join(texts[:5])
        st.markdown(f"> {combined_texts}")
