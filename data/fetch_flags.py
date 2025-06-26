#!/usr/bin/env python3

import os
import requests
import pandas as pd
from minio import Minio
from minio.error import S3Error
import io
import json

# --- KONFIGURASI ---

# Daftar negara yang Anda berikan
COUNTRIES = [
    'Italy', 'France', 'Turkey', 'Indonesia', 'Saudi Arabia', 'USA', 'Nigeria', 
    'Australia', 'Canada', 'Mexico', 'China', 'South Africa', 'Japan', 'UK', 
    'Russia', 'Brazil', 'Germany', 'India', 'Argentina', 'South Korea'
]

# Konfigurasi MinIO (ambil dari environment variables, sama seperti script Spark Anda)
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_NAME = "flags" # Nama bucket untuk menyimpan gambar bendera

# --- FUNGSI-FUNGSI ---

def get_flag_url(country_name):
    """Mengambil URL gambar bendera PNG dari API REST Countries."""
    try:
        # Menggunakan API v3.1 untuk mendapatkan data negara berdasarkan nama
        response = requests.get(f"https://restcountries.com/v3.1/name/{country_name}")
        response.raise_for_status()  # Akan error jika status code bukan 2xx
        data = response.json()
        # Mengambil URL bendera dalam format PNG dari data JSON
        flag_png_url = data[0]['flags']['png']
        print(f"✅ Berhasil mendapatkan URL bendera untuk: {country_name}")
        return flag_png_url
    except requests.exceptions.RequestException as e:
        print(f"❌ Gagal mendapatkan URL untuk {country_name}: {e}")
        return None

def create_flag_url_dataset(countries):
    """Membuat DataFrame Pandas berisi nama negara dan URL benderanya."""
    print("Tahap 1: Membuat dataset URL bendera...")
    flag_data = []
    for country in countries:
        url = get_flag_url(country)
        if url:
            flag_data.append({"Country": country, "Flag_URL": url})
    
    df = pd.DataFrame(flag_data)
    return df

def download_and_upload_to_minio(minio_client, bucket_name, country, flag_url):
    """Mengunduh gambar dari URL dan mengunggahnya ke MinIO."""
    try:
        # 1. Mengunduh gambar
        print(f"  Downloading bendera untuk {country} dari {flag_url[:30]}...")
        image_response = requests.get(flag_url, stream=True)
        image_response.raise_for_status()
        
        # Membaca konten gambar sebagai byte stream
        image_data = image_response.content
        image_stream = io.BytesIO(image_data)
        
        # 2. Menyiapkan nama file dan mengunggah ke MinIO
        # Contoh: "South Korea" -> "South Korea.png"
        object_name = f"{country}.png"
        content_type = image_response.headers.get('Content-Type', 'image/png')
        
        print(f"  Uploading {object_name} ke bucket '{bucket_name}' di MinIO...")
        minio_client.put_object(
            bucket_name,
            object_name,
            image_stream,
            length=len(image_data),
            content_type=content_type
        )
        print(f"  ✅ Upload {object_name} berhasil!")
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ Gagal mengunduh bendera untuk {country}: {e}")
    except S3Error as e:
        print(f"  ❌ Gagal mengunggah bendera untuk {country} ke MinIO: {e}")


# --- EKSEKUSI UTAMA ---

if __name__ == "__main__":
    # TAHAP 1: Membuat dataset URL
    flags_df = create_flag_url_dataset(COUNTRIES)
    
    print("\n--- Dataset URL Bendera yang Berhasil Dibuat ---")
    print(flags_df)
    
    # Menyimpan dataset ke file CSV lokal sebagai bukti
    flags_df.to_csv("data/flags_dataset.csv", index=False)
    print("\nDataset telah disimpan ke 'data/flags_dataset.csv'")
    
    # TAHAP 2: Mengunduh dan Menyimpan ke MinIO
    print("\nTahap 2: Mengunduh gambar dan menyimpan ke MinIO...")
    
    # Inisialisasi klien MinIO
    try:
        client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=False # Set ke True jika MinIO Anda menggunakan HTTPS
        )
    except Exception as e:
        print(f"FATAL: Gagal terhubung ke MinIO di {MINIO_ENDPOINT}. Error: {e}")
        exit(1)

    # Cek apakah bucket sudah ada, jika tidak, buat baru dan atur policy
    try:
        found = client.bucket_exists(MINIO_BUCKET_NAME)
        if not found:
            print(f"Bucket '{MINIO_BUCKET_NAME}' tidak ditemukan. Membuat bucket baru...")
            client.make_bucket(MINIO_BUCKET_NAME)
            print(f"Bucket '{MINIO_BUCKET_NAME}' berhasil dibuat.")
        else:
            print(f"Bucket '{MINIO_BUCKET_NAME}' sudah ada.")

        # Mengatur policy bucket agar bisa diakses publik (read-only)
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{MINIO_BUCKET_NAME}/*"],
                },
            ],
        }
        client.set_bucket_policy(MINIO_BUCKET_NAME, json.dumps(policy))
        print(f"Policy publik (read-only) berhasil diatur untuk bucket '{MINIO_BUCKET_NAME}'.")

    except S3Error as e:
        print(f"FATAL: Error saat memeriksa/membuat bucket di MinIO. Error: {e}")
        exit(1)

    # Iterasi melalui DataFrame dan jalankan proses download-upload
    for index, row in flags_df.iterrows():
        download_and_upload_to_minio(
            client, 
            MINIO_BUCKET_NAME, 
            row["Country"], 
            row["Flag_URL"]
        )
        
    print("\n===================================")
    print("SEMUA PROSES SELESAI!")
    print(f"Silakan cek bucket '{MINIO_BUCKET_NAME}' di MinIO Anda.")
    print("===================================")