import json
import time
import os
import sys
import threading
from datetime import datetime
from urllib.parse import urlparse, urlunparse
from kafka import KafkaConsumer
from hdfs import InsecureClient
import requests
from requests.adapters import HTTPAdapter

# ==========================================
# IMPORT KONFIGURASI
# ==========================================
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import (
    KAFKA_BROKER, KAFKA_API_VERSION,
    TOPIC_KURS, TOPIC_PANGAN,
    HDFS_WEB_URL, HDFS_USER,
    BRONZE_KURS_PATH, BRONZE_PANGAN_PATH
)

# ==========================================
# KONFIGURASI DASHBOARD LOKAL (SPEED LAYER)
# ==========================================
LOCAL_KURS_LIVE = "dashboard/data/live_kurs.json"
LOCAL_PANGAN_LIVE = "dashboard/data/live_pangan.json"

# ==========================================
# KONEKSI HDFS (DENGAN DOCKER REWRITE ADAPTER)
# ==========================================
class _DockerHostRewriteAdapter(HTTPAdapter):
    """Mengatasi masalah redirect IP DataNode Docker ke localhost"""
    def send(self, request, **kwargs):
        parsed = urlparse(request.url)
        if parsed.hostname not in ('localhost', '127.0.0.1', None):
            port = parsed.port
            new_netloc = f'localhost:{port}' if port else 'localhost'
            request.url = urlunparse(parsed._replace(netloc=new_netloc))
        return super().send(request, **kwargs)


def make_hdfs_client():
    session = requests.Session()
    session.mount('http://', _DockerHostRewriteAdapter())
    return InsecureClient(HDFS_WEB_URL, user=HDFS_USER, session=session)


hdfs_client = make_hdfs_client()

# ==========================================
# INISIALISASI FILE LOKAL
# ==========================================
def init_local_files():
    """Memastikan folder dan file cache dashboard lokal siap digunakan"""
    os.makedirs("dashboard/data", exist_ok=True)
    for path in [LOCAL_KURS_LIVE, LOCAL_PANGAN_LIVE]:
        if not os.path.exists(path):
            with open(path, 'w', encoding='utf-8') as f:
                json.dump([], f)

# ==========================================
# FUNGSI KONSUMER UTAMA
# ==========================================
def process_topic(topic_name, hdfs_dir, live_file_path):
    print(f"[{topic_name}] Consumer aktif dan mendengarkan pesan dari Kafka...")

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=KAFKA_BROKER,
        api_version=KAFKA_API_VERSION,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id=f'lakehouse_writer_{topic_name}',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    buffer = []
    last_save_time = time.time()
    flush_interval = 60  # Flush ke HDFS setiap 60 detik

    for message in consumer:
        data = message.value
        buffer.append(data)

        # -------------------------------------------------------------
        # 1. SPEED LAYER: Update cache lokal seketika untuk Dashboard
        # -------------------------------------------------------------
        try:
            with open(live_file_path, 'r', encoding='utf-8') as f:
                live_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            live_data = []

        live_data.insert(0, data)
        live_data = live_data[:140]  # Simpan hingga 140 rekaman (histori cukup untuk 7 komoditas)

        try:
            with open(live_file_path, 'w', encoding='utf-8') as f:
                json.dump(live_data, f, indent=4)
        except Exception as e:
            print(f"[{topic_name}] Gagal update Speed Layer lokal: {e}")

        # -------------------------------------------------------------
        # 2. BATCH LAYER: Kemas buffer ke HDFS Bronze Lakehouse
        # -------------------------------------------------------------
        current_time = time.time()

        # Flush jika sudah 60 detik ATAU sudah terkumpul minimal 10 pesan
        if (current_time - last_save_time >= flush_interval) or (len(buffer) >= 10):
            if len(buffer) > 0:
                timestamp_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
                file_name = f"{timestamp_str}.json"
                hdfs_path = f"{hdfs_dir}{file_name}"

                print(f"[{topic_name}] Mengemas {len(buffer)} pesan ke {file_name}...")

                try:
                    hdfs_client.makedirs(hdfs_dir)
                    json_data = json.dumps(buffer, indent=4)

                    with hdfs_client.write(hdfs_path, encoding='utf-8') as writer:
                        writer.write(json_data)

                    print(f"[{topic_name}] ✅ Berhasil disimpan di HDFS: {hdfs_path}")

                    # Reset buffer HANYA jika penyimpanan ke HDFS berhasil
                    buffer = []
                    last_save_time = time.time()
                except Exception as e:
                    print(f"[{topic_name}] ❌ Gagal menulis ke HDFS (buffer dipertahankan untuk retry): {e}")

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print("Menjalankan Lakehouse Dual-Routing Consumer...")
    init_local_files()

    # Jalankan consumer Kurs dan consumer Pangan secara independen di dua thread terpisah
    t1 = threading.Thread(target=process_topic, args=(TOPIC_KURS, BRONZE_KURS_PATH, LOCAL_KURS_LIVE), daemon=True)
    t2 = threading.Thread(target=process_topic, args=(TOPIC_PANGAN, BRONZE_PANGAN_PATH, LOCAL_PANGAN_LIVE), daemon=True)

    t1.start()
    t2.start()

    try:
        t1.join()
        t2.join()
    except KeyboardInterrupt:
        print("\nConsumer dihentikan oleh pengguna.")


if __name__ == "__main__":
    main()