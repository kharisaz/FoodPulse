"""
Flask Dashboard — FoodPulse
Real-Time Macro-to-Micro Food Security Intelligence & Early Warning System
Akses di: http://localhost:8080
"""

import json
import os
import sys
from datetime import datetime
from flask import Flask, render_template, jsonify

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import KOMODITAS_METADATA

app = Flask(__name__)

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
GOLD_DIR    = os.path.join(DATA_DIR, "gold")
LIVE_KURS   = os.path.join(DATA_DIR, "live_kurs.json")
LIVE_PANGAN = os.path.join(DATA_DIR, "live_pangan.json")


def _read_json(path, default):
    try:
        if not os.path.exists(path) or os.path.getsize(path) == 0:
            return default
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


# ── Halaman Utama ────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


# ── API: Speed Layer (Data Streaming Terkini) ─────────────────────────────────
@app.route("/api/kurs")
def get_kurs():
    records = _read_json(LIVE_KURS, [])
    if records:
        records.sort(key=lambda r: r.get("timestamp", ""))

        latest      = records[-1]
        current     = latest.get("rate", 0)
        current_oil = latest.get("oil_price_usd", None)

        history = [
            {
                "date": r.get("timestamp", ""),
                "rate": r.get("rate", 0),
                "oil_price_usd": r.get("oil_price_usd", None)
            }
            for r in records[-30:]
        ]
    else:
        current     = 0
        current_oil = None
        history     = []

    return jsonify({
        "current_rate": current,
        "current_oil": current_oil,
        "timestamp": datetime.now().isoformat(),
        "history": history,
        "source": "live" if records else "empty"
    })


@app.route("/api/pangan")
def get_pangan():
    records = _read_json(LIVE_PANGAN, [])

    # Kelompokkan rekaman berdasarkan komoditas untuk melacak histori harga stream
    history_by_kom = {}
    for r in records:
        k = r.get("komoditas", "")
        if k:
            history_by_kom.setdefault(k, []).append(r)

    items = []
    for k, hist in history_by_kom.items():
        meta = KOMODITAS_METADATA.get(k, {})
        latest_r = hist[0]
        harga = latest_r.get("harga", 0)
        het = meta.get("het", 0)

        # Hitung perubahan harga (vs stream sebelumnya atau deviasi vs HET)
        if len(hist) >= 2 and hist[1].get("harga"):
            prev_h = hist[1].get("harga")
            change_pct = round(((harga - prev_h) / prev_h) * 100, 2)
        elif het:
            change_pct = round(((harga - het) / het) * 100, 2)
        else:
            change_pct = 0.0

        items.append({
            "name": k,
            "price": harga,
            "date": latest_r.get("tanggal", ""),
            "kategori": latest_r.get("kategori", meta.get("kategori", "Umum")),
            "satuan": meta.get("satuan", "kg"),
            "het": het,
            "perubahan_pct": change_pct
        })

    return jsonify({
        "items": items,
        "timestamp": datetime.now().isoformat(),
        "source": "live" if records else "empty"
    })


# ── API: Gold Layer (Batch & AI Analytics Engine) ─────────────────────────────
@app.route("/api/monthly_kurs")
def get_monthly_kurs():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "monthly_kurs.json"), []))


@app.route("/api/monthly_pangan")
def get_monthly_pangan():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "monthly_pangan.json"), {}))


@app.route("/api/correlation")
def get_correlation():
    data = _read_json(os.path.join(GOLD_DIR, "correlation.json"), [])
    overall = round(sum(d["pearson_corr_kurs"] for d in data) / len(data), 4) if data else None
    return jsonify({
        "by_komoditas": data,
        "overall_avg": overall,
        "timestamp": datetime.now().isoformat()
    })


@app.route("/api/analytics")
def get_analytics():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "analytics.json"), {"komoditas": []}))


@app.route("/api/forecast")
def get_forecast():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "forecast.json"), {"model_info": {"status": "NO_DATA"}, "forecasts": []}))


@app.route("/api/anomaly")
def get_anomaly():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "anomaly.json"), {"anomalies": [], "summary": {"total_anomalies": 0}}))


@app.route("/api/het_warning")
def get_het_warning():
    return jsonify(_read_json(os.path.join(GOLD_DIR, "het_warning.json"), {"warnings": []}))

# ── API: Speed Layer Live In-Flight Alerts ────────────────────────────────────
@app.route("/api/live_alerts")
def get_live_alerts():
    kurs_records = _read_json(LIVE_KURS, [])
    pangan_records = _read_json(LIVE_PANGAN, [])
    alerts = []

    # 1. Evaluasi Kejutan Kurs Seketika (Volatilitas Kurs Cepat)
    if len(kurs_records) >= 2:
        kurs_records.sort(key=lambda r: r.get("timestamp", ""))
        latest_kurs = kurs_records[-1].get("rate", 0)
        prev_kurs = kurs_records[-2].get("rate", latest_kurs)
        diff_pct = ((latest_kurs - prev_kurs) / prev_kurs) * 100 if prev_kurs else 0

        # Jika ada pergerakan kurs signifikan atau level kurs melonjak tajam
        if abs(diff_pct) >= 0.25 or latest_kurs >= 18600:
            alerts.append({
                "id": "macro_kurs_shock",
                "type": "danger" if (diff_pct >= 0.5 or latest_kurs >= 19000) else "warning",
                "badge": "SPEED LAYER: KURS MACRO SHOCK",
                "title": f"Tekanan Kurs USD/IDR Terkini: Rp {latest_kurs:,.0f} ({diff_pct:+.2f}%)",
                "message": (
                    f"Nilai tukar berada pada tren tinggi. Model regresi Spark memproyeksikan "
                    f"risiko transmisi kenaikan harga pada komoditas berkandungan impor tinggi "
                    f"(Bawang Putih Impor & Minyak Goreng) dalam rentang waktu 14–21 hari ke depan."
                ),
                "action": "Rekomendasi Kebijakan: Koordinasi TPID daerah & evaluasi kuota izin impor pangan.",
                "timestamp": kurs_records[-1].get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            })

    # 2. Evaluasi Pelanggaran HET Seketika dari Live Pangan Bapanas
    latest_pangan = {}
    for r in pangan_records:
        k = r.get("komoditas", "")
        if k and k not in latest_pangan:
            latest_pangan[k] = r

    for komoditas, rec in latest_pangan.items():
        meta = KOMODITAS_METADATA.get(komoditas)
        if not meta:
            continue
        het = meta.get("het", 0)
        harga = rec.get("harga", 0)

        if het > 0 and harga > het:
            breach_pct = ((harga - het) / het) * 100
            alerts.append({
                "id": f"het_breach_{komoditas}",
                "type": "danger" if breach_pct >= 15 else "warning",
                "badge": "SPEED LAYER: LIVE HET BREACH",
                "title": f"Pelanggaran HET Terdeteksi: {komoditas}",
                "message": (
                    f"Harga pasar saat ini Rp {harga:,.0f} telah melampaui HET resmi pemerintah "
                    f"Rp {het:,.0f} (+{breach_pct:.1f}% di atas batas regulasi)."
                ),
                "action": "Rekomendasi Kebijakan: Gelar Operasi Pasar Murah (OPM) dan percepat distribusi pasokan SPHP.",
                "timestamp": rec.get("tanggal", datetime.now().strftime("%Y-%m-%d"))
            })

    return jsonify({
        "status": "ALERT" if alerts else "NORMAL",
        "total_alerts": len(alerts),
        "alerts": alerts,
        "timestamp": datetime.now().isoformat()
    })

# ── API: Ringkasan KPI Eksekutif (Gabungan Speed + Gold) ─────────────────────
@app.route("/api/summary")
def get_summary():
    kurs_records = _read_json(LIVE_KURS, [])
    analytics    = _read_json(os.path.join(GOLD_DIR, "analytics.json"), {"komoditas": []})
    anomaly      = _read_json(os.path.join(GOLD_DIR, "anomaly.json"), {"summary": {}})
    het          = _read_json(os.path.join(GOLD_DIR, "het_warning.json"), {"warnings": []})

    # Urutkan berdasarkan waktu agar selalu mendapat kurs paling mutakhir
    if kurs_records:
        kurs_records.sort(key=lambda r: r.get("timestamp", ""))
        current_rate = kurs_records[-1].get("rate", 0)
        current_oil  = kurs_records[-1].get("oil_price_usd", None)
    else:
        current_rate = 0
        current_oil  = None

    top_fpvi = analytics["komoditas"][0] if analytics.get("komoditas") else None
    kritis_het = [
        w for w in het.get("warnings", [])
        if "KRITIS" in w.get("status", "").upper() or "DARURAT" in w.get("status", "").upper()
    ]

    return jsonify({
        "current_kurs": current_rate,
        "current_oil": current_oil,
        "top_vulnerable": top_fpvi,
        "total_het_breach": len(kritis_het),
        "total_anomalies": anomaly.get("summary", {}).get("total_anomalies", 0),
        "high_severity_anomalies": anomaly.get("summary", {}).get("high_severity", 0),
        "timestamp": datetime.now().isoformat()
    })

# ── API: Pipeline & Cluster Telemetry (Ops Health Check) ─────────────────────
@app.route("/api/pipeline_health")
def get_pipeline_health():
    import socket

    # 1. Cek Koneksi Socket Kafka Broker (Port 9092)
    kafka_up = False
    try:
        with socket.create_connection(("localhost", 9092), timeout=0.4):
            kafka_up = True
    except Exception:
        kafka_up = False

    # 2. Cek Koneksi Socket HDFS NameNode (Port 9870 atau 8020)
    hdfs_up = False
    try:
        with socket.create_connection(("localhost", 9870), timeout=0.4):
            hdfs_up = True
    except Exception:
        try:
            with socket.create_connection(("localhost", 8020), timeout=0.4):
                hdfs_up = True
        except Exception:
            hdfs_up = False

    # 3. Cek Kesegaran Stream Speed Layer (Lag Streaming)
    kurs_records = _read_json(LIVE_KURS, [])
    speed_status = "OFFLINE"
    lag_text = "No Signal"

    if kurs_records:
        kurs_records.sort(key=lambda r: r.get("timestamp", ""))
        latest_ts = kurs_records[-1].get("timestamp", "")
        try:
            latest_dt = datetime.strptime(latest_ts, "%Y-%m-%d %H:%M:%S")
            diff_sec = int((datetime.now() - latest_dt).total_seconds())
            if diff_sec <= 60:
                speed_status = "STREAMING"
                lag_text = f"Lag {diff_sec}s"
            elif diff_sec <= 300:
                speed_status = "IDLE"
                lag_text = f"{int(diff_sec/60)}m lalu"
            else:
                speed_status = "STANDBY"
                lag_text = "> 5m lalu"
        except Exception:
            speed_status = "ACTIVE"
            lag_text = "Live"

    # 4. Cek Kesiapan Model PySpark Gold Layer
    gold_analytics = os.path.join(GOLD_DIR, "analytics.json")
    gold_forecast = os.path.join(GOLD_DIR, "forecast.json")
    spark_ready = os.path.exists(gold_analytics) and os.path.exists(gold_forecast)

    return jsonify({
        "kafka": {
            "status": "UP" if kafka_up else "DOWN",
            "port": 9092,
            "topics": ["sembako-stream", "kurs-stream"]
        },
        "hdfs": {
            "status": "UP" if hdfs_up else "DOWN",
            "port": 9870,
            "layer": "Lakehouse Bronze/Silver"
        },
        "speed_layer": {
            "status": speed_status,
            "lag": lag_text
        },
        "spark_gold": {
            "status": "TRAINED & READY" if spark_ready else "PENDING",
            "models_count": 6
        },
        "timestamp": datetime.now().isoformat()
    })

# ── Main Entry Point ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(GOLD_DIR, exist_ok=True)
    print("Menjalankan FoodPulse Serving Dashboard di http://localhost:8080 ...")
    app.run(host="0.0.0.0", port=8080, debug=True, use_reloader=False)