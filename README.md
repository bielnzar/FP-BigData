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

Proyek ini membangun sebuah platform analitik data end-to-end untuk menganalisis disparitas kesehatan global. Dengan memanfaatkan dataset terstruktur mengenai statistik kesehatan global dan data tidak terstruktur dari abstrak medis, platform ini bertujuan untuk mengidentifikasi hubungan antara faktor sosio-ekonomi, infrastruktur kesehatan, dan beban penyakit di berbagai negara. Arsitektur modern berbasis teknologi open-source seperti Apache Kafka, Apache Spark, Delta Lake di atas MinIO, Trino, Flask, dan Streamlit diorkestrasi menggunakan Docker. Alur kerja mencakup ingest data secara real-time, pemrosesan ETL dengan Spark, penyimpanan di Data Lakehouse (medallion architecture), pelatihan model machine learning untuk prediksi indikator kesehatan, dan penyajian wawasan melalui dashboard interaktif. Tujuan akhirnya adalah menyediakan alat bantu bagi para pembuat kebijakan untuk merancang intervensi kesehatan yang lebih efektif dan tepat sasaran.

## Platform Analitik Prediktif untuk Pengentasan Disparitas Kesehatan Global

Dataset yang digunakan 
- Terstuktur: [Global Health Statistics](https://www.kaggle.com/datasets/malaiarasugraj/global-health-statistics)
- Tidak Terstruktur: [Medical Abstract Classification Dataset](https://www.kaggle.com/datasets/viswaprakash1990/medical-abstract-classification-dataset)

## Pendahuluan

### Latar Belakang Masalah

Kesehatan global menghadapi tantangan fundamental berupa kesenjangan (disparitas) yang tajam antara negara maju dan negara berkembang. Negara-negara berpenghasilan rendah dan menengah (LMICs) seringkali menanggung "beban ganda penyakit": mereka masih berjuang mengatasi penyakit menular (seperti TBC dan Malaria) sambil menghadapi peningkatan pesat penyakit tidak menular (seperti penyakit jantung dan diabetes). Kesenjangan ini diduga kuat berakar pada faktor sosio-ekonomi dan keterbatasan akses terhadap layanan kesehatan yang berkualitas. Untuk merancang intervensi yang efektif, para pembuat kebijakan memerlukan wawasan yang mendalam dari data, namun seringkali terhalang oleh volume dan kompleksitas data kesehatan global.

### Rumusan Masalah

Proyek ini bertujuan untuk menjawab pertanyaan utama: Bagaimana faktor-faktor sosio-ekonomi (misalnya, pendapatan per kapita, indeks pendidikan) dan infrastruktur kesehatan (misalnya, jumlah dokter, akses layanan) secara kolektif memengaruhi beban penyakit yang berbeda (diukur dengan angka kematian dan DALYs) di berbagai negara?

### Tujuan Proyek

1. Merancang dan mendeskripsikan arsitektur platform analitik Big Data yang modern, skalabel, dan efisien menggunakan teknologi open-source.

2. Mengimplementasikan pipeline data untuk memproses dataset "Global Health Statistics".

3. Mengembangkan model Machine Learning untuk memprediksi indikator kesehatan kunci berdasarkan faktor-faktor tertentu.

4. Membangun dashboard interaktif untuk visualisasi data dan penyajian hasil prediksi kepada user.

## Landasan Teknologi

Untuk membangun platform ini, digunakan serangkaian teknologi open-source yang telah menjadi standar industri dalam rekayasa data dan ilmu data.

- Apache Kafka: Berfungsi sebagai sistem messaging terdistribusi yang berperan sebagai "pintu masuk" utama untuk data. Kafka mampu menangani aliran data bervolume tinggi secara real-time.

- Apache Spark: Merupakan mesin komputasi terpadu untuk pemrosesan data skala besar. Spark adalah "otak" dari pipeline ini, digunakan untuk semua tugas ETL (Extract, Transform, Load), analisis data, dan pelatihan model machine learning.

- MinIO: Digunakan sebagai fondasi penyimpanan Data Lake. MinIO adalah sistem object storage berkinerja tinggi yang kompatibel dengan Amazon, memungkinkan penyimpanan data dalam skala besar dengan biaya yang efektif.

- Delta Lake: Merupakan lapisan penyimpanan transaksional yang berjalan di atas MinIO. Delta Lake memberikan keandalan transaksi ACID, tata kelola skema, dan performa pada Data Lake, mengatasi banyak keterbatasan pada data lake tradisional.

- Trino: Mesin kueri SQL terdistribusi yang dioptimalkan untuk analitik interaktif berlatensi rendah. Trino berfungsi sebagai "jembatan" kueri yang cepat antara Data Lake dan dashboard.

- Flask: Kerangka kerja web mikro yang ringan dan fleksibel, digunakan secara spesifik untuk menyajikan model Machine Learning yang telah dilatih sebagai sebuah REST API yang efisien.

- Streamlit: Kerangka kerja Python untuk membangun aplikasi web dan dashboard interaktif dengan cepat, memungkinkan visualisasi data dan hasil model tanpa memerlukan pengembangan front-end yang kompleks.

- Docker & Docker Compose: Docker Compose digunakan untuk mendefinisikan dan menjalankan layanan aplikasi tersebut secara bersamaan dengan satu perintah, menyederhanakan pengembangan dan deployment.

## Arsitektur dan Metodologi

### Diagram Arsitektur Solusi

![Image-Arsitektur](https://github.com/bielnzar/FP-BigData/blob/main/images/revisi-arsitektur3.png)

## Penjelasan Rinci Alur Kerja

- Ingest (Pengumpulan Data): Sebuah skrip Python (Publisher) membaca dataset `.csv/.json/.txt` dan mempublikasikannya baris per baris sebagai pesan ke Apache Kafka untuk mensimulasikan aliran data.

- Pemrosesan oleh Spark: Apache Spark membaca data dari Kafka. Spark melakukan proses ETL (membersihkan, mengubah, dan memperkaya data) dan menyimpannya ke Delta Lake di atas MinIO dengan struktur medallion (Bronze, Silver, Gold).

- Pelatihan Model: Spark juga bertanggung jawab untuk melatih model Machine Learning menggunakan data bersih dari zona Silver di Data Lake. Model yang sudah jadi disajikan melalui Flask API.

- Kueri oleh Trino: Trino terhubung ke Data Lake (Delta Lake di MinIO) untuk menyediakan akses kueri SQL yang sangat cepat. Trino dirancang khusus untuk beban kerja analitik interaktif, membuatnya sempurna untuk dashboard.

- Penyajian Aplikasi: Streamlit Dashboard mengambil data untuk visualisasi (grafik, peta, tabel) dengan menjalankan kueri ke Trino. Saat pengguna membutuhkan prediksi, dashboard akan memanggil Flask API di belakang layar. Keduanya dijalankan sebagai container Docker yang terpisah.

- Interaksi Pengguna: Pengguna akhir (analis, pembuat kebijakan) berinteraksi hanya dengan Streamlit Dashboard yang menyajikan gabungan dari visualisasi data historis dan prediksi dari model.