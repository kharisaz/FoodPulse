# ==========================================
# 1. KONFIGURASI KAFKA BROKER & TOPIK
# ==========================================
KAFKA_BROKER = ['localhost:9092']
KAFKA_API_VERSION = (3, 9, 0)

TOPIC_KURS = 'kurs-usd-idr'
TOPIC_PANGAN = 'harga-pangan'

import os

# ==========================================
# 2. KONFIGURASI HADOOP HDFS
# ==========================================
HDFS_WEB_URL = os.getenv("HDFS_WEB_URL", "http://localhost:9870")  # WebHDFS untuk Python 'hdfs'
HDFS_RPC_URL = os.getenv("HDFS_RPC_URL", "hdfs://localhost:8020")  # RPC untuk PySpark
HDFS_USER    = os.getenv("HDFS_USER", "root")

# Path Direktori Lakehouse (WebHDFS / Ingestion Layer)
BRONZE_KURS_PATH   = "/data/lakehouse/bronze/kurs/"
BRONZE_PANGAN_PATH = "/data/lakehouse/bronze/pangan/"
BRONZE_EIA_PATH    = "/data/lakehouse/bronze/eia/"
BRONZE_BPS_PATH    = "/data/lakehouse/bronze/bps/"

# Path RPC untuk PySpark (Bronze -> Silver -> Gold)
HDFS_BRONZE_KURS   = f"{HDFS_RPC_URL}{BRONZE_KURS_PATH}"
HDFS_BRONZE_PANGAN = f"{HDFS_RPC_URL}{BRONZE_PANGAN_PATH}"
HDFS_BRONZE_EIA    = f"{HDFS_RPC_URL}{BRONZE_EIA_PATH}"
HDFS_BRONZE_BPS    = f"{HDFS_RPC_URL}{BRONZE_BPS_PATH}"

HDFS_SILVER_PATH   = f"{HDFS_RPC_URL}/data/lakehouse/silver"
HDFS_GOLD_PATH     = f"{HDFS_RPC_URL}/data/lakehouse/gold"

# Alias kompatibilitas
SILVER_PATH = HDFS_SILVER_PATH
GOLD_PATH   = HDFS_GOLD_PATH

# ==========================================
# 3. METADATA 6 KOMODITAS STRATEGIS NASIONAL
# ==========================================
KOMODITAS_METADATA = {
    "Bawang Putih Impor": {
        "kategori": "Impor Murni",
        "bobot_impor": 1.0,
        "het": 40000,
        "satuan": "kg"
    },
    "Daging Ayam Ras": {
        "kategori": "Semi-Impor (Pakan)",
        "bobot_impor": 0.6,
        "het": 38000,
        "satuan": "kg"
    },
    "Telur Ayam Ras": {
        "kategori": "Semi-Impor (Pakan)",
        "bobot_impor": 0.6,
        "het": 29500,
        "satuan": "kg"
    },
    "Minyak Goreng Kemasan": {
        "kategori": "Terkendali HET",
        "bobot_impor": 0.3,
        "het": 17500,
        "satuan": "liter"
    },
    "Beras Medium": {
        "kategori": "Pangan Pokok",
        "bobot_impor": 0.2,
        "het": 13500,
        "satuan": "kg"
    },
    "Cabai Rawit Merah": {
        "kategori": "Volatile Lokal",
        "bobot_impor": 0.1,
        "het": 57000,
        "satuan": "kg"
    },
    "Bawang Merah": {
        "kategori": "Volatile Lokal",
        "bobot_impor": 0.1,
        "het": 41500,
        "satuan": "kg"
    }
}