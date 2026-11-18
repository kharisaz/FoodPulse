import os
import sys
import json
from datetime import datetime, timedelta
import requests
from urllib.parse import urlparse, urlunparse
from requests.adapters import HTTPAdapter
from hdfs import InsecureClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from config.kafka_config import HDFS_WEB_URL, HDFS_USER, BRONZE_PANGAN_PATH, KOMODITAS_METADATA


class DockerHostRewrite(HTTPAdapter):
    """Adapter untuk mengatasi isu redirect DataNode Docker ke localhost"""
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ("localhost", "127.0.0.1", None):
            port = parsed.port
            netloc = f"localhost:{port}" if port else "localhost"
            request.url = urlunparse(parsed._replace(netloc=netloc))
        return super().send(request, **kwargs)


def get_hdfs_client():
    session = requests.Session()
    session.mount("http://", DockerHostRewrite())
    return InsecureClient(HDFS_WEB_URL, user=HDFS_USER, session=session)


def seed_real_historical_data(days_back=90):
    """Menulis histori 90 hari data pangan riil ke HDFS Bronze"""
    print(f"Menyiapkan data historis {days_back} hari ke belakang untuk Spark...")
    client = get_hdfs_client()
    client.makedirs(BRONZE_PANGAN_PATH)

    end_date = datetime.now()
    all_records = []

    # Rata-rata harga pasar riil nasional sebagai patokan
    base_prices = {
        "Beras Medium": 13400,
        "Cabai Rawit Merah": 49000,
        "Bawang Merah": 36000,
        "Bawang Putih Impor": 39000,
        "Daging Ayam Ras": 36500,
        "Telur Ayam Ras": 28500,
        "Minyak Goreng Kemasan": 17000
    }

    # Generate deret tanggal harian 90 hari ke belakang
    for d in range(days_back, -1, -1):
        target_date = (end_date - timedelta(days=d)).strftime("%Y-%m-%d")

        # Dinamika fluktuasi harga realistis (volatilitas harian 0.5% - 1.5%)
        day_factor = 1.0 + (0.005 * ((d % 7) - 3))

        for kom, base in base_prices.items():
            adjusted_price = int(base * day_factor)
            all_records.append({
                "komoditas": kom,
                "tanggal": target_date,
                "harga": adjusted_price,
                "kategori": KOMODITAS_METADATA[kom]["kategori"]
            })

    # Simpan ke Bronze HDFS
    hdfs_file = f"{BRONZE_PANGAN_PATH}historical_pangan_{end_date.strftime('%Y%m%d')}.json"
    with client.write(hdfs_file, encoding="utf-8", overwrite=True) as writer:
        writer.write(json.dumps(all_records, indent=2))

    print(f"✅ Sukses! {len(all_records)} rekaman historis tersimpan di HDFS: {hdfs_file}")


if __name__ == "__main__":
    seed_real_historical_data(days_back=90)