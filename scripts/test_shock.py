#!/usr/bin/env python3
"""
FoodPulse — Live Market Shock & Real-Time Pipeline Validator
Gunakan skrip ini untuk menguji secara langsung responsivitas Speed Layer,
aliran streaming Kafka, deteksi anomali in-flight, dan alarm dashboard.

Contoh Penggunaan:
  # 1. Injeksi lonjakan harga komoditas (Pelanggaran HET)
  python scripts/test_shock.py --komoditas "Cabai Rawit Merah" --harga 85000

  # 2. Injeksi lonjakan kurs makro ekstrem
  python scripts/test_shock.py --kurs 19500

  # 3. Kembalikan kondisi pasar ke normal (STREAM STABLE)
  python scripts/test_shock.py --reset
"""

import os
import sys
import json
import argparse
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KAFKA_BROKER, TOPIC_PANGAN, TOPIC_KURS, KOMODITAS_METADATA

LOCAL_KURS_LIVE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dashboard/data/live_kurs.json"))
LOCAL_PANGAN_LIVE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../dashboard/data/live_pangan.json"))

def _write_local_cache(file_path: str, record: dict, max_items: int = 140):
    """Fallback langsung ke Speed Layer Cache jika Kafka broker sedang offline"""
    try:
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if os.path.exists(file_path):
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = []
        data.insert(0, record)
        data = data[:max_items]
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        return True
    except Exception as e:
        print(f"[WARN] Gagal update cache lokal: {e}")
        return False

def send_payload(topic: str, record: dict, local_cache_path: str):
    kafka_success = False
    try:
        from kafka import KafkaProducer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=3000
        )
        producer.send(topic, value=record)
        producer.flush(timeout=3)
        producer.close()
        kafka_success = True
        print(f"✅ [KAFKA STREAM] Event tersalurkan ke broker topic '{topic}'!")
    except Exception as e:
        print(f"⚠️ [KAFKA BYPASS] Broker Kafka offline ({e}).")
        print(f"⚡ Mengarahkan event langsung ke Speed Layer Cache (Speed Layer Direct Ingestion)...")

    # Pastikan data juga langsung tersimpan di cache lokal dashboard
    _write_local_cache(local_cache_path, record)
    if not kafka_success:
        print("✅ Speed Layer Cache lokal diperbarui seketika.")

def inject_pangan_shock(komoditas: str, harga: int):
    if komoditas not in KOMODITAS_METADATA:
        print(f"⚠️ Komoditas '{komoditas}' tidak ada di metadata.")
        print(f"Pilihan valid: {list(KOMODITAS_METADATA.keys())}")
        return

    meta = KOMODITAS_METADATA[komoditas]
    het = meta.get("het", 0)
    dev_pct = ((harga - het) / het) * 100 if het else 0

    record = {
        "komoditas": komoditas,
        "tanggal": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "harga": int(harga),
        "kategori": meta.get("kategori", "Lokal")
    }

    send_payload(TOPIC_PANGAN, record, LOCAL_PANGAN_LIVE)

    print("\n" + "="*65)
    print("🚀 [TEST SHOCK: PANGAN] HASIL INJEKSI EVENT")
    print("="*65)
    print(f"• Komoditas   : {komoditas} ({meta.get('kategori')})")
    print(f"• Harga Injeksi: Rp {harga:,} (HET: Rp {het:,} | Deviasi: {dev_pct:+.1f}%)")
    print(f"• Timestamp   : {record['tanggal']}")
    print("="*65)
    print("👉 CEK DASHBOARD ANDA DI BROWSER:")
    if harga > het:
        print(f"   🔴 Top Banner berubah MERAH: '🚨 SPEED LAYER: LIVE HET BREACH - {komoditas}'")
        print(f"   🔴 Counter Pelanggaran HET seketika bertambah.")
        print(f"   🔴 Kartu '{komoditas}' menampilkan delta lonjakan harga ekstrem ({dev_pct:+.1f}%).\n")
    else:
        print(f"   🟢 Harga normal di bawah HET. Banner tetap hijau (STREAM STABLE).\n")

def inject_kurs_shock(rate: float):
    oil_price = 95.80
    record = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "rate": float(rate),
        "beli": round(float(rate) * 0.998, 2),
        "jual": round(float(rate) * 1.002, 2),
        "oil_price_usd": oil_price,
        "source": "Manual Shock Injection Test"
    }

    send_payload(TOPIC_KURS, record, LOCAL_KURS_LIVE)

    print("\n" + "="*65)
    print("🚀 [TEST SHOCK: KURS MAKRO] HASIL INJEKSI EVENT")
    print("="*65)
    print(f"• Nilai Kurs  : Rp {rate:,.2f}")
    print(f"• Minyak Brent: ${oil_price}/BBL")
    print(f"• Timestamp   : {record['timestamp']}")
    print("="*65)
    print("👉 CEK DASHBOARD ANDA DI BROWSER:")
    print("   📈 Kartu KURS USD-IDR seketika melompat ke angka baru.")
    print("   📈 Grafik Tren Dual-Makro langsung menunjukkan spike vertikal.")
    if rate >= 18600:
        print("   ⚠️ Top Banner berubah KUNING/MERAH: 'SPEED LAYER: KURS MACRO SHOCK'.\n")

def reset_to_normal():
    print("\n🔄 Mengembalikan seluruh indikator pasar ke kondisi normal...")
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Reset seluruh komoditas ke 92% HET (Aman terkendali)
    for k, meta in KOMODITAS_METADATA.items():
        normal_price = int(meta["het"] * 0.92)
        rec = {
            "komoditas": k,
            "tanggal": now_str,
            "harga": normal_price,
            "kategori": meta.get("kategori", "Lokal")
        }
        send_payload(TOPIC_PANGAN, rec, LOCAL_PANGAN_LIVE)

    # Reset kurs ke rentang normal
    normal_kurs = {
        "timestamp": now_str,
        "rate": 18265.0,
        "beli": 18228.47,
        "jual": 18301.53,
        "oil_price_usd": 76.5,
        "source": "Global Spot Market (Normalized)"
    }
    send_payload(TOPIC_KURS, normal_kurs, LOCAL_KURS_LIVE)

    print("="*65)
    print("✅ Seluruh pasar dinormalkan! Banner Speed Layer kembali HIJAU (STREAM STABLE).")
    print("="*65 + "\n")

def main():
    parser = argparse.ArgumentParser(description="FoodPulse Real-Time Pipeline Shock Validator")
    parser.add_argument("--komoditas", type=str, help="Nama komoditas yang akan diinjeksi shock (misal: 'Cabai Rawit Merah')")
    parser.add_argument("--harga", type=int, help="Harga komoditas shock (misal: 85000)")
    parser.add_argument("--kurs", type=float, help="Nilai kurs USD/IDR shock (misal: 19500)")
    parser.add_argument("--reset", action="store_true", help="Reset pasar ke kondisi normal (STREAM STABLE)")

    args = parser.parse_args()

    if args.reset:
        reset_to_normal()
    elif args.komoditas and args.harga:
        inject_pangan_shock(args.komoditas, args.harga)
    elif args.kurs:
        inject_kurs_shock(args.kurs)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
