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
    - ***Catatan tentang Deduplikasi***: *Sistem ini menerapkan mekanisme deduplikasi pada tahap Bronze ke Silver dengan menggunakan hash unik untuk setiap baris data. Hal ini menjamin bahwa data di lapisan Silver dan Gold selalu konsisten dan bebas dari duplikasi, bahkan jika data yang sama di-ingest berulang kali.*

- **Pelatihan Model (Spark & Flask)**:
    - Spark job (`train_model.py`) menggunakan data dari lapisan Silver untuk melatih model Machine Learning. Model yang digunakan adalah **Random Forest Regressor** dari library Spark MLlib, yang cocok untuk tugas prediksi regresi. Model yang telah dilatih kemudian disimpan ke MinIO.
    - Sebuah Flask API (`api/app.py`) memuat model tersebut dan menyediakannya melalui endpoint `/predict`.

- **Penyajian & Visualisasi (DuckDB & Streamlit)**:
    - Dashboard Streamlit (`dashboard/`) berfungsi sebagai antarmuka pengguna.
    - DuckDB, yang berjalan di dalam container Streamlit, terhubung langsung ke lapisan **Gold** di MinIO untuk menjalankan kueri analitik secara cepat dan efisien.
    - Saat pengguna berinteraksi, dashboard mengambil data untuk visualisasi (grafik, tabel) dengan menjalankan kueri via DuckDB. Untuk prediksi, dashboard memanggil Flask API di belakang layar.

## Struktur Folder
```
FP-BigData/
├── data/
│   ├── Global Health Statistics.csv  # (Dihasilkan oleh skrip)
│   ├── medical_text_classification_fake_dataset.csv # (Dihasilkan oleh skrip)
│   ├── download_dataset.sh
│   └── fetch_flags.py
├── docker-compose.yml
├── README.md
├── run_pipeline_automation.sh
├── images/
│   └── Arsitektur-Fiks.png
└── src/
    ├── api/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   └── app.py
    ├── consumer/
    │   ├── Dockerfile
    │   ├── requirements.txt
    │   ├── consumer.py
    │   └── start_consumers.sh
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

## Langkah Penggunaan

### 1. Persiapan Awal
- **Clone Repository**:
  ```bash
  git clone https://github.com/bielnzar/FP-BigData.git
  cd FP-BigData
  ```
- **Jalankan Platform**: Perintah ini akan membangun image Docker dan menjalankan semua layanan (Kafka, Spark, MinIO, dll.) di latar belakang.
  ```bash
  docker compose up -d --build
  ```

### 2. Menjalankan Pipeline (Cara Otomatis - Direkomendasikan)
Ini adalah cara termudah untuk menjalankan seluruh pipeline dari awal hingga akhir.

1.  **Unduh Dataset**: Jalankan skrip ini untuk mengunduh dataset yang diperlukan ke folder `data/`.
    ```bash
    bash data/download_dataset.sh
    ```

2.  **Unduh Gambar Bendera (Data Tidak Terstruktur)**: Jalankan skrip ini untuk mengunduh bendera negara dan menyimpannya di MinIO.
    ```bash
    python3 data/fetch_flags.py
    ```

3.  **Kirim Data ke Kafka (Producer)**: Jalankan skrip ini untuk mengirim semua data dari file CSV ke Kafka. Ini hanya perlu dijalankan sekali di awal untuk mengisi pipeline.
    ```bash
    bash src/producer/start_producers.sh
    ```
    *Tunggu hingga skrip ini selesai (biasanya beberapa menit).*

4.  **Jalankan Otomatisasi Pipeline**: Skrip ini akan menjalankan konsumen dan pekerjaan Spark secara berkala.
    ```bash
    bash run_pipeline_automation.sh
    ```
    *Biarkan terminal ini berjalan. Skrip ini akan terus memproses data, melatih model, dan memperbarui data mart setiap jam (default). Tekan `Ctrl+C` untuk menghentikannya dengan aman.*

### 3. Menjalankan Pipeline (Langkah Manual - Untuk Development/Debugging)
Gunakan langkah-langkah ini jika Anda ingin menjalankan setiap komponen secara terpisah untuk tujuan pengembangan atau pemecahan masalah.

1.  **Unduh Dataset & Bendera**: Ikuti langkah 1 & 2 dari cara otomatis di atas.

2.  **Jalankan Producer**:
    ```bash
    bash src/producer/start_producers.sh
    ```

3.  **Jalankan Consumer**: Buka terminal baru, jalankan skrip ini, dan biarkan tetap berjalan.
    ```bash
    bash src/consumer/start_consumers.sh
    ```

4.  **Jalankan Spark Jobs**: Jalankan pekerjaan Spark secara berurutan.
    ```bash
    # 1. Proses data dari Bronze ke Silver
    bash src/spark_jobs/run_spark_job.sh bronze_to_silver.py

    # 2. Proses data dari Silver ke Gold (agregasi)
    bash src/spark_jobs/run_spark_job.sh silver_to_gold.py

    # 3. Latih model Machine Learning
    bash src/spark_jobs/run_spark_job.sh train_model.py
    ```
    *Catatan: Setelah melatih model, restart layanan API agar model yang baru dapat dimuat: `docker compose restart api`*

### 4. Akses Aplikasi
- **Dashboard Analitik**: Buka browser dan akses Streamlit Dashboard di `http://localhost:8501`.
- **MinIO Console**: Untuk melihat file di Data Lake, akses `http://localhost:9001` (Login: `minioadmin`/`minioadmin`).
- **Flask API Health Check**: Untuk memeriksa status model di API, akses `http://localhost:5001/health`.

## DOKUMENTASI

### Dashboard Interface

![interface-1](https://github.com/bielnzar/FP-BigData/blob/main/images/interface-1.png)
![interface-1](https://github.com/bielnzar/FP-BigData/blob/main/images/interface-2.png)
![interface-1](https://github.com/bielnzar/FP-BigData/blob/main/images/interface-3.png)
![interface-1](https://github.com/bielnzar/FP-BigData/blob/main/images/interface-4.png)
![interface-1](https://github.com/bielnzar/FP-BigData/blob/main/images/interface-5.png)

### Tools Dashboard

![minio](https://github.com/bielnzar/FP-BigData/blob/main/images/minio.png)
![spark-master](https://github.com/bielnzar/FP-BigData/blob/main/images/spark-master.png)
![spark-worker](https://github.com/bielnzar/FP-BigData/blob/main/images/spark-worker.png)
![health-api](https://github.com/bielnzar/FP-BigData/blob/main/images/health-api.png)

