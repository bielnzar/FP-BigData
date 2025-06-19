#!/bin/bash

create_dir_if_not_exists() {
    local dir_path="$1"
    if [ ! -d "${dir_path}" ]; then
        echo "Creating directory: ${dir_path}"
        mkdir -p "${dir_path}"
    else
        echo "Directory already exists: ${dir_path}"
    fi
}

echo "Starting project directory setup..."

# Direktori Utama
PROJECT_ROOT="." # Atau ganti dengan path absolut jika perlu

# 1. Direktori Data
create_dir_if_not_exists "${PROJECT_ROOT}/data"
create_dir_if_not_exists "${PROJECT_ROOT}/data/raw"
create_dir_if_not_exists "${PROJECT_ROOT}/data/processed"

# 2. Direktori Notebooks
# create_dir_if_not_exists "${PROJECT_ROOT}/notebooks"

# 3. Direktori Sumber Kode (src)
create_dir_if_not_exists "${PROJECT_ROOT}/src"
create_dir_if_not_exists "${PROJECT_ROOT}/src/publishers"
create_dir_if_not_exists "${PROJECT_ROOT}/src/spark_jobs"
create_dir_if_not_exists "${PROJECT_ROOT}/src/flask_api"
create_dir_if_not_exists "${PROJECT_ROOT}/src/flask_api/models"
create_dir_if_not_exists "${PROJECT_ROOT}/src/streamlit_app"
create_dir_if_not_exists "${PROJECT_ROOT}/src/streamlit_app/pages"

# 4. Direktori Konfigurasi
# create_dir_if_not_exists "${PROJECT_ROOT}/config"

# 5. Direktori Model Lokal (jaga" dev sebelum integrasi MinIO penuh)
create_dir_if_not_exists "${PROJECT_ROOT}/models"
create_dir_if_not_exists "${PROJECT_ROOT}/models/health_predictor_model"

# 6. Direktori Tests
create_dir_if_not_exists "${PROJECT_ROOT}/tests"

# 7. Direktori untuk GitHub Actions
create_dir_if_not_exists "${PROJECT_ROOT}/.github"
create_dir_if_not_exists "${PROJECT_ROOT}/.github/workflows"

# Membuat beberapa file penting
echo "Creating placeholder/important files..."

# .gitignore
if [ ! -f "${PROJECT_ROOT}/.gitignore" ]; then
    echo "Creating .gitignore"
    cat << EOF > "${PROJECT_ROOT}/.gitignore"
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
#  Usually these files are written by a python script from a template
#  before PyInstaller builds the exe, so as to inject date/other infos into it.
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/

# Translations
*.mo
*.pot

# Django stuff:
*.log
local_settings.py
db.sqlite3
db.sqlite3-journal

# Flask stuff:
instance/
.webassets-cache

# Scrapy stuff:
.scrapy

# Sphinx documentation
docs/_build/

# PyBuilder
target/

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# PEP 582; __pypackages__
__pypackages__/

# Celery stuff
celerybeat-schedule
celerybeat.pid

# SageMath files
.sage/

# Environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# Spyder project settings
.spyderproject
.spyproject

# Rope project settings
.ropeproject

# mkdocs documentation
/site

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# pytype static analyzer
.pytype/

# Cython debug symbols
cython_debug/

# VS Code
.vscode/

# Docker
docker-compose.override.yml
docker-compose.local.yml

# Data files (jika besar dan tidak ingin di-commit)
# data/raw/*.csv
# data/raw/*.json
# data/processed/*.parquet

# Model files (jika besar dan tidak ingin di-commit)
# models/**/*.pkl
# models/**/*.h5

# Log files
*.log
logs/

# Temporary files
*.tmp
*.swp
*~
EOF
else
    echo ".gitignore already exists."
fi

# README.md (jika belum ada)
if [ ! -f "${PROJECT_ROOT}/README.md" ]; then
    echo "Creating README.md"
    echo "# Final Project - Big Data dan Data Lakehouse" > "${PROJECT_ROOT}/README.md"
    echo "" >> "${PROJECT_ROOT}/README.md"
    echo "## Pendahuluan" >> "${PROJECT_ROOT}/README.md"
else
    echo "README.md already exists."
fi

# .env.example (jika belum ada)
if [ ! -f "${PROJECT_ROOT}/.env.example" ]; then
    echo "Creating .env.example"
    cat << EOF > "${PROJECT_ROOT}/.env.example"
# MinIO Credentials
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=admin
MINIO_SECRET_KEY=password123
MINIO_BUCKET_NAME=fp-bigdata

# Kafka Brokers
KAFKA_BROKERS=localhost:9092 # Atau alamat dari Docker Compose jika berbeda

# NewsAPI (jika digunakan)
# NEWSAPI_KEY=your_news_api_key

# Spark Configuration
# SPARK_MASTER_URL=spark://localhost:7077 # Atau alamat dari Docker Compose

# Flask API
# FLASK_APP=src/flask_api/app.py
# FLASK_ENV=development

# Streamlit
# STREAMLIT_SERVER_PORT=8501
EOF
else
    echo ".env.example already exists."
fi


echo "Project directory setup finished."
echo "Current project structure (top level):"
ls -lA "${PROJECT_ROOT}"