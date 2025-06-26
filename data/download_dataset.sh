#!/bin/bash

# Dapatkan direktori tempat skrip ini berada
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Pindah ke direktori skrip untuk memastikan file diunduh di lokasi yang benar
cd "$SCRIPT_DIR"

echo "=== Script Download Dataset ==="
echo "Memastikan file diunduh ke: $(pwd)"

check_kaggle_cli() {
    if ! command -v kaggle &> /dev/null; then
        echo "❌ Kaggle CLI tidak ditemukan!"
        echo "Silakan install kaggle CLI terlebih dahulu:"
        echo "pip install kaggle"
        echo ""
        echo "Setelah install, konfigurasi API key:"
        echo "1. Buka https://www.kaggle.com/account"
        echo "2. Scroll ke 'API' section"
        echo "3. Klik 'Create New API Token'"
        echo "4. Download kaggle.json"
        echo "5. Pindahkan ke ~/.kaggle/kaggle.json"
        echo "6. Set permission: chmod 600 ~/.kaggle/kaggle.json"
        exit 1
    fi
}

check_kaggle_config() {
    if [ ! -f ~/.kaggle/kaggle.json ]; then
        echo "❌ File kaggle.json tidak ditemukan di ~/.kaggle/"
        echo "Silakan konfigurasi API key terlebih dahulu"
        exit 1
    fi
}

echo "🔍 Mengecek Kaggle CLI..."
check_kaggle_cli
check_kaggle_config
echo "✅ Kaggle CLI siap digunakan"

echo ""
echo "📥 Mengunduh Global Health Statistics Dataset..."
kaggle datasets download -d malaiarasugraj/global-health-statistics --unzip

if [ $? -eq 0 ]; then
    echo "✅ Global Health Statistics Dataset berhasil diunduh"
else
    echo "❌ Gagal mengunduh Global Health Statistics Dataset"
    exit 1
fi

echo ""
echo "📥 Mengunduh Medical Abstract Classification Dataset..."
kaggle datasets download -d viswaprakash1990/medical-abstract-classification-dataset --unzip

if [ $? -eq 0 ]; then
    echo "✅ Medical Abstract Classification Dataset berhasil diunduh"
else
    echo "❌ Gagal mengunduh Medical Abstract Classification Dataset"
    exit 1
fi

echo ""
echo "📊 Menampilkan isi folder saat ini:"
ls -la .

echo ""
echo "🎉 Proses download selesai!"
echo "Dataset tersimpan di folder $(pwd)"
