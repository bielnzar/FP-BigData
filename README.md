# Final Project 

### Mata Kuliah Big Data dan Data Lakehouse (A)

### _Kelompok 6_

|      **Nama**       |  **NRP**   |
| :-----------------: | :--------: |
|    Nabiel Nizar Anwari        | 5027231087 |
|        Hasan                  | 5027231073 |
| Muhammad Kenas Galeno Putra   | 5027231069 |
| Muhammad Hildan Adiwena       | 5027231077 |
| Mochamad Fadhil Saifullah      | 5027231068 |

## Abstrak

Proyek ini membangun sebuah platform analitik data end-to-end untuk menganalisis disparitas kesehatan global. Dengan memanfaatkan dataset terstruktur mengenai statistik kesehatan global, data semi-terstruktur dari abstrak medis, dan data tidak terstruktur berupa gambar bendera, platform ini bertujuan untuk mengidentifikasi hubungan antara faktor sosio-ekonomi, infrastruktur kesehatan, dan beban penyakit di berbagai negara. Arsitektur modern berbasis teknologi open-source seperti Apache Kafka, Apache Spark, MinIO, DuckDB, Flask, dan Streamlit diorkestrasi menggunakan Docker. Alur kerja mencakup ingest data, pemrosesan ETL dengan Spark, penyimpanan dalam Data Lakehouse (arsitektur medallion dengan format Parquet), pelatihan model machine learning untuk prediksi angka kematian, dan penyajian wawasan melalui dashboard interaktif. Tujuan akhirnya adalah menyediakan alat bantu bagi para pembuat kebijakan untuk merancang intervensi kesehatan yang lebih efektif dan tepat sasaran.

## Platform Analitik Prediktif untuk Pengentasan Disparitas Kesehatan Global

Dataset yang digunakan:
- **Terstruktur**: [Global Health Statistics](https://www.kaggle.com/datasets/malaiarasugraj/global-health-statistics) - Data statistik kesehatan dalam format CSV.
- **Semi-Terstruktur**: [Medical Abstract Classification Dataset](https://www.kaggle.com/datasets/viswaprakash1990/medical-abstract-classification-dataset) - Teks abstrak medis dengan label kategori dalam format JSON/CSV.
- **Tidak Terstruktur**: Gambar Bendera Negara - Dihasilkan secara dinamis melalui skrip `data/fetch_flags.py` dari API eksternal dan disimpan sebagai file PNG di MinIO.

## Pendahuluan

### Latar Belakang Masalah

Kesehatan global menghadapi tantangan fundamental berupa kesenjangan (disparitas) yang tajam antara negara maju dan negara berkembang. Negara-negara berpenghasilan rendah dan menengah (LMICs) seringkali menanggung "beban ganda penyakit": mereka masih berjuang mengatasi penyakit menular (seperti TBC dan Malaria) sambil menghadapi peningkatan pesat penyakit tidak menular (seperti penyakit jantung dan diabetes). Kesenjangan ini diduga kuat berakar pada faktor sosio-ekonomi dan keterbatasan akses terhadap layanan kesehatan yang berkualitas. Untuk merancang intervensi yang efektif, para pembuat kebijakan memerlukan wawasan yang mendalam dari data, namun seringkali terhalang oleh volume dan kompleksitas data kesehatan global.

### Rumusan Masalah

Proyek ini bertujuan untuk menjawab pertanyaan utama: Bagaimana faktor-faktor sosio-ekonomi (misalnya, pendapatan per kapita, indeks pendidikan) dan infrastruktur kesehatan (misalnya, jumlah dokter, akses layanan) secara kolektif memengaruhi beban penyakit yang berbeda (diukur dengan angka kematian dan DALYs) di berbagai negara?

### Tujuan Proyek

1. Merancang dan mendeskripsikan arsitektur platform analitik Big Data yang modern, skalabel, dan efisien menggunakan teknologi open-source.

2. Mengimplementasikan pipeline data untuk memproses dataset "Global Health Statistics" dan "Medical Abstracts".

3. Mengembangkan model Machine Learning untuk memprediksi indikator kesehatan kunci berdasarkan faktor-faktor tertentu.

4. Membangun dashboard interaktif untuk visualisasi data dan penyajian hasil prediksi kepada user.

## Landasan Teknologi

Untuk membangun platform ini, digunakan serangkaian teknologi open-source yang telah menjadi standar industri dalam rekayasa data dan ilmu data.

- Apache Kafka: Berfungsi sebagai sistem messaging terdistribusi yang berperan sebagai "pintu masuk" utama untuk data. Kafka mampu menangani aliran data bervolume tinggi secara real-time.

- Apache Spark: Merupakan mesin komputasi terpadu untuk pemrosesan data skala besar. Spark adalah "otak" dari pipeline ini, digunakan untuk semua tugas ETL (Extract, Transform, Load), analisis data, dan pelatihan model machine learning.

- MinIO: Digunakan sebagai fondasi penyimpanan Data Lake. MinIO adalah sistem object storage berkinerja tinggi yang kompatibel dengan Amazon S3, memungkinkan penyimpanan data dalam skala besar dengan biaya yang efektif.

- Data Lakehouse (Parquet): Kami menerapkan arsitektur Medallion (Bronze, Silver, Gold) di atas MinIO. Data disimpan dalam format Parquet, sebuah format kolom yang efisien dan teroptimasi untuk beban kerja analitik.

- DuckDB: Merupakan database analitik dalam-proses (in-process) yang sangat cepat. Dalam arsitektur ini, DuckDB berjalan langsung di dalam container Streamlit, berfungsi sebagai mesin kueri yang sangat efisien untuk membaca data Parquet langsung dari MinIO untuk analisis interaktif di dashboard. Ini menyederhanakan arsitektur secara signifikan.

- Flask: Kerangka kerja web mikro yang ringan dan fleksibel, digunakan secara spesifik untuk menyajikan model Machine Learning yang telah dilatih sebagai sebuah REST API yang efisien.

- Streamlit: Kerangka kerja Python untuk membangun aplikasi web dan dashboard interaktif dengan cepat, memungkinkan visualisasi data dan hasil model tanpa memerlukan pengembangan front-end yang kompleks.

- Docker & Docker Compose: Docker Compose digunakan untuk mendefinisikan dan menjalankan layanan aplikasi tersebut secara bersamaan dengan satu perintah, menyederhanakan pengembangan dan deployment.

## Arsitektur dan Metodologi

### Diagram Arsitektur Solusi

![Image-Arsitektur](https://github.com/bielnzar/FP-BigData/blob/main/Arsitektur-Fiks.png)

## Penjelasan Rinci Alur Kerja

- **Ingest (Pengumpulan Data)**: Skrip Python (`producer.py`) membaca dataset CSV dan mempublikasikannya baris per baris sebagai pesan ke topik Apache Kafka, mensimulasikan aliran data.

- **Landing**: Skrip Python (`consumer.py`) mendengarkan topik Kafka, mengumpulkan pesan dalam batch, dan menyimpannya sebagai file JSONL di lapisan **Bronze** pada Data Lake MinIO.

- **Pemrosesan ETL (Spark)**:
    - **Bronze ke Silver**: Spark job (`bronze_to_silver.py`) membaca data mentah dari lapisan Bronze, melakukan pembersihan (casting tipe data, penanganan null), standardisasi, dan deduplikasi, lalu menyimpannya dalam format Parquet di lapisan **Silver**.
    - **Silver ke Gold**: Spark job (`silver_to_gold.py`) membaca data bersih dari lapisan Silver, melakukan agregasi dan transformasi bisnis untuk membuat tabel-tabel analitik (misalnya, ringkasan per negara), dan menyimpannya di lapisan **Gold** dalam format Parquet.

- **Pelatihan Model (Spark & Flask)**:
    - Spark job (`train_model.py`) menggunakan data dari lapisan Silver untuk melatih model Machine Learning (RandomForest) dan menyimpan model yang telah dilatih ke MinIO.
    - Sebuah Flask API (`api/app.py`) memuat model tersebut dan menyediakannya melalui endpoint `/predict`.

- **Penyajian & Visualisasi (DuckDB & Streamlit)**:
    - Dashboard Streamlit (`dashboard/`) berfungsi sebagai antarmuka pengguna.
    - DuckDB, yang berjalan di dalam container Streamlit, terhubung langsung ke lapisan **Gold** di MinIO untuk menjalankan kueri analitik secara cepat dan efisien.
    - Saat pengguna berinteraksi, dashboard mengambil data untuk visualisasi (grafik, tabel) dengan menjalankan kueri via DuckDB. Untuk prediksi, dashboard memanggil Flask API di belakang layar.

## Langkah Penggunaan

### 1. Persiapan Awal
- **Clone Repository**:
  ```bash
  git clone https://github.com/bielnzar/FP-BigData.git
  cd FP-BigData
  ```
- **Download Dataset**: Buat folder `data` dan letakkan file `Global_Health_Statistics.csv` dan `Medical_Abstracts.csv` di dalamnya.
  ```bash
  mkdir -p data
  # Letakkan file CSV secara manual di folder data/
  ```
- **Jalankan Platform**: Perintah ini akan membangun image Docker dan menjalankan semua layanan (Kafka, Spark, MinIO, dll.).
  ```bash
  docker-compose up -d --build
  ```

### 2. Alur Kerja Data
1.  **Unduh Gambar Bendera (Opsional, sekali jalan)**: Jalankan skrip ini untuk mengunduh bendera negara dan menyimpannya di MinIO.
    ```bash
    python3 data/fetch_flags.py
    ```

2.  **Kirim Data ke Kafka (Producer)**: Buka terminal baru dan jalankan skrip untuk memulai kedua producer secara otomatis.
    ```bash
    bash src/producer/start_producers.sh
    ```
    *Skrip ini akan membuat ulang topik Kafka dan mulai mengirim data dari file CSV di folder `data/` ke Kafka.*

3.  **Simpan Data ke MinIO (Consumer)**: Buka dua terminal baru dan jalankan consumer untuk setiap topik.
    ```bash
    # Di terminal pertama, jalankan consumer untuk statistik kesehatan
    python3 src/consumer/consumer.py --topic global-health-stats --group health-consumer-group --batch-size 1000 --batch-timeout 60

    # Di terminal kedua, jalankan consumer untuk abstrak medis
    python3 src/consumer/consumer.py --topic medical-abstracts --group abstract-consumer-group --batch-size 500 --batch-timeout 60
    ```
    *Biarkan consumer berjalan untuk menangkap aliran data dari Kafka dan menyimpannya ke lapisan Bronze di MinIO. Hentikan dengan `Ctrl+C` jika sudah selesai.*

4.  **Proses Data dengan Spark**: Jalankan Spark jobs secara berurutan menggunakan skrip yang disediakan.
    ```bash
    # 1. Proses data dari Bronze ke Silver
    bash src/spark_jobs/run_spark_job.sh bronze_to_silver.py

    # 2. Proses data dari Silver ke Gold (agregasi)
    bash src/spark_jobs/run_spark_job.sh silver_to_gold.py

    # 3. Latih model Machine Learning
    bash src/spark_jobs/run_spark_job.sh train_model.py
    ```
    *Catatan: Setelah melatih model, restart layanan API agar model yang baru dapat dimuat: `docker-compose restart flask-api`*

### 3. Akses Aplikasi
- **Dashboard Analitik**: Buka browser dan akses Streamlit Dashboard di `http://localhost:8501`.
- **MinIO Console**: Untuk melihat file di Data Lake, akses `http://localhost:9001` (Login: `minioadmin`/`minioadmin`).
- **Flask API Health Check**: Untuk memeriksa status model di API, akses `http://localhost:5001/health`.

## Struktur Folder

```
FP-BigData/
├── data/
│   ├── Global_Health_Statistics.csv  # (Download dulu)
│   ├── Medical_Abstracts.csv         # (Download dulu)
│   ├── download_dataset.sh
│   └── fetch_flags.py
├── docker-compose.yml
├── README.md
├── images/
│   └── revisi-arsitektur3.png
└── src/
    ├── api/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── app.py
    ├── consumer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── consumer.py
    ├── dashboard/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── app.py
    │   ├── utils.py
    │   └── pages/
    │       ├── 1_Country_Summary.py
    │       ├── 2_Yearly_Trends.py
    │       ├── 3_Abstract_Analysis.py
    │       └── 4_Predictive_Model.py
    ├── producer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── producer.py
    │   └── start_producers.sh
    └── spark_jobs/
        ├── Dockerfile
        ├── requirements.txt
        ├── bronze_to_silver.py
        ├── silver_to_gold.py
        ├── train_model.py
        └── run_spark_job.sh
```