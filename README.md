# FoodPulse: Real-Time Macro-to-Micro Food Security Intelligence & Early Warning Platform

> **Author & Lead Architect:** Khumaidi Kharis Az-zacky  
> **Architecture:** Distributed Lambda Architecture & Medallion Data Lakehouse  
> **Core Stack:** Apache Kafka (KRaft) • Apache Hadoop (HDFS) • Apache Spark (PySpark ML) • Flask • Chart.js  
> **Domain:** Macroeconomic Shock Propagation, Commodity Price Resilience, & Algorithmic Market Surveillance  

---

## 1. Executive Summary & Problem Space

Ketahanan pangan Indonesia memiliki ketergantungan asimetris terhadap dinamika ekonomi global (*macroeconomic shocks*). Pelemahan nilai tukar Rupiah terhadap Dolar AS (USD/IDR) secara langsung mengerek biaya impor komoditas pangan pokok (seperti Bawang Putih yang 95% kuotanya berasal dari impor) serta bahan baku industri pendukung (gandum dan bungkil kedelai untuk pakan ternak unggas). Di saat yang sama, fluktuasi harga energi dunia (*Brent Crude Oil*) langsung mentransmisikan tekanan pada biaya pengapalan logistik internasional dan ongkos distribusi antar pulau.

### Keterbatasan Sistem Pemantauan Pangan Konvensional:
1. **Pasif dan Terlambat (*Post-Mortem Reporting*):** Portal harga pangan konvensional umumnya hanya berfungsi sebagai papan pencatat statistik kejadian masa lalu (*lagging indicator*). Ketika data kenaikan harga dipublikasikan, inflasi pangan sudah terlanjur terjadi dan mengikis daya beli masyarakat miskin.
2. **Ketiadaan Hubungan Kausal Makro ke Mikro (*Macro-Blindness*):** Sistem yang ada memperlakukan harga pasar eceran seolah terisolasi dari faktor eksternal, tanpa memodelkan dinamika transmisi dari bursa valuta asing dan komoditas energi dunia.
3. **Ketidakmampuan Mengidentifikasi Akar Masalah:** Ketika harga beras atau cabai melonjak tajam, pembuat kebijakan kesulitan membedakan:
   * Apakah kenaikan tersebut **wajar (*cost-push inflation*)** akibat pelemahan kurs dan lonjakan BBM?
   * Ataukah merupakan **anomali pasar non-makro** akibat penimbunan spekulan, disrupsi rantai pasok lokal, atau manipulasi kartel pangan?

---

## 2. Gagasan & Arsitektur Solusi: FoodPulse

**FoodPulse** dirancang sebagai platform intelijen pangan generasi baru yang mentransformasi pemantauan sembako menjadi **Sistem Deteksi Dini & Kewaspadaan Dini (*Early Warning System*)** berskala *enterprise*.

Platform ini mengintegrasikan **Distributed Lambda Architecture** dengan **Medallion Data Lakehouse**:
* **Batch Processing Layer (Deep AI Intelligence):** Menyerap dan memproses data historis 90+ hari melalui zona *Bronze $\rightarrow$ Silver $\rightarrow$ Gold* di Hadoop HDFS. Memanfaatkan **PySpark ML** untuk melatih model regresi multi-variat, mengkalkulasi korelasi transmisi *Dynamic Time-Lag* (0, 7, 14, 21 hari), merumuskan indeks kerentanan pangan (**FPVI 2.0**), serta mengisolasi anomali pasar menggunakan analisis **Residual Regression ($Z \ge 2.0$)**.
* **Speed Processing Layer (Real-Time Ingestion & Alerting):** Mengalirkan pergerakan spot kurs valas 24/5 dan bursa minyak mentah secara live melalui **Apache Kafka**, disandingkan dengan harga pasar harian dari API resmi Badan Pangan Nasional (Bapanas). Sistem secara otomatis mengevaluasi kejutan pasar seketika (*in-flight*) untuk membunyikan alarm peringatan dini sebelum harga menembus batas Harga Eceran Tertinggi (HET).
* **Serving & Control Layer (Interactive Command Center):** Menyajikan panel analitik eksekutif, simulator dampak transmisi, dan *telemetry monitor* kesehatan klaster Big Data secara interaktif di `http://localhost:8080`.

---

## 3. Keranjang Komoditas Pangan Strategis

FoodPulse memantau keranjang 7 komoditas pangan pokok dengan metadata acuan regulasi nasional:

| Komoditas | HET Resmi | Satuan | Bobot Impor ($W$) | Kategori Pasar | Karakteristik Transmisi Shock |
| :--- | :---: | :---: | :---: | :--- | :--- |
| **Beras Medium** | Rp 13.500 | kg | 15% | Pangan Pokok | Pangan pokok utama dengan elastisitas harga rendah; diproteksi cadangan Bulog (SPHP). |
| **Bawang Putih Impor** | Rp 40.000 | kg | 95% | Impor Murni | Sensitivitas tertinggi terhadap pelemahan kurs USD/IDR dan regulasi kuota impor. |
| **Daging Ayam Ras** | Rp 38.000 | kg | 40% | Semi-Impor | Transmisi bertahap melalui biaya impor bahan baku pakan ternak (jagung & bungkil kedelai). |
| **Telur Ayam Ras** | Rp 29.500 | kg | 35% | Semi-Impor | Sensitif terhadap biaya pakan unggas komersial dan ongkos logistik distribusi. |
| **Minyak Goreng Kemasan** | Rp 17.500 | liter | 20% | Terkendali HET | Terikat kebijakan DMO sawit; komponen kemasan dan transportasi terdampak minyak global. |
| **Cabai Rawit Merah** | Rp 57.000 | kg | 5% | Volatile Lokal | Fluktuasi tinggi akibat cuaca & panen lokal; transmisi makro dominan pada ongkos angkut BBM. |
| **Bawang Merah** | Rp 41.500 | kg | 5% | Volatile Lokal | Rentan siklus musiman sentra produksi lokal; biaya logistik distribusi dipengaruhi BBM. |

---

## 4. Arsitektur Terdistribusi & Alur Pipeline

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     FOODPULSE PIPELINE FLOW                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘

1. DATA INGESTION ENGINE
   ├─► [STREAMING] producer_kurs.py    ──► Live Global Spot USD/IDR & Brent Oil ──► Kafka: kurs-stream
   ├─► [STREAMING] producers_pangan.py ──► API Badan Pangan Nasional (Bapanas)  ──► Kafka: sembako-stream
   └─► [BATCH HIST] fetch_*.py         ──► 90-Hari Historis BI, Bapanas, EIA, BPS ──► Direct to HDFS Bronze

2. SPEED LAYER (Low-Latency Processing & Alerting)
   └─► consumer_to_hdfs.py:
       ├─► In-Memory Stream Cache      ──► dashboard/data/live_*.json (Sub-detik)
       ├─► In-Flight Shock Evaluator   ──► Flash Volatility Alert & Live HET Breach Banner
       └─► Micro-Batch Lakehouse Flush ──► HDFS /data/lakehouse/bronze/ (Interval 60s / 10 pesan)

3. BATCH LAYER: MEDALLION LAKEHOUSE (Apache Spark 3.x Engine)
   ├─► Bronze Layer (bronze_layer.py):
   │   └─ Ingest raw JSON dari HDFS ──► Validasi skema data ──► Parquet Bronze terkompresi
   │
   ├─► Silver Layer (silver_layer.py):
   │   └─ Data cleaning, forward-fill windowing (gap weekend), & join multi-dimensi waktu
   │
   └─► Gold Layer (gold_layer.py - AI & Advanced Analytics Engine):
       ├─► PySpark ML Multi-Variate Linear Regression (Kurs + Minyak Dunia ──► Proyeksi 4 Minggu)
       ├─► Dynamic Time-Lag Analysis (Evaluasi koefisien transmisi lag 0, 7, 14, 21 hari)
       ├─► FPVI 2.0 (Food Price Vulnerability Index: Bobot Impor + Korelasi + Volatilitas)
       ├─► Residual-Based Anomaly Detection (Z-Score Residual >= 2.0: Deteksi Spekulan vs Cost-Push)
       └─► HET Runway Alert (Estimasi sisa minggu sebelum ambang regulasi terlampaui)

4. SERVING & COMMAND CENTER (Flask Web Engine @ Port 8080)
   ├─► Executive KPI Summary & Live In-Flight Warning Ticker
   ├─► Dual-Axis Macro Trends Visualization (Kurs USD/IDR vs Brent Crude Oil)
   ├─► 7 Strategic Commodities Live Prices & Volatilitas Deviasi
   ├─► PySpark ML Forecasting & Model Evaluation Dashboard (R² Score & RMSE)
   ├─► Interactive Expandable FPVI Table (Analisis Detail Transmisi Shock)
   └─► Live Cluster & Pipeline Telemetry (Heartbeat Kafka, HDFS NameNode, Speed Lag, Spark Status)
```

---

## 5. Struktur Direktori Proyek

```text
sembako_watch/
├── config/
│   └── kafka_config.py          # Konfigurasi sentral: Kafka Broker, HDFS URL, Topik, Metadata HET
├── producers/
│   ├── streaming_kafka/
│   │   ├── producer_kurs.py     # Dual-Macro Streamer (Live Spot USD/IDR + Brent Crude)
│   │   └── producers_pangan.py  # Real-time Streamer Panel Harga Bapanas
│   └── batch_direct/
│       ├── fetch_historical_bapanas.py  # Ingesti 90-hari historis pangan resmi ke HDFS Bronze
│       ├── fetch_kurs_bi.py             # Ingesti 90-hari historis kurs transaksi BI ke HDFS Bronze
│       ├── fetch_oil_eia.py             # Ingesti historis minyak mentah EIA ke HDFS Bronze
│       └── fetch_macro_bps.py           # Ingesti historis inflasi bulanan BPS ke HDFS Bronze
├── consumers/
│   └── consumer_to_hdfs.py      # Dual-Routing Consumer: Speed Layer Cache & HDFS Bronze Lakehouse
├── spark/
│   ├── bronze_layer.py          # Ekstraksi Raw JSON ke Parquet Bronze Lakehouse
│   ├── silver_layer.py          # Cleansing, Forward-Fill Windowing, & Multi-variate Time Join
│   └── gold_layer.py            # PySpark ML Regressor, Dynamic Lag, FPVI 2.0, & Residual Anomaly
├── dashboard/
│   ├── app.py                   # Serving API Engine (Port 8080) + Telemetry & Alerting Evaluator
│   ├── templates/
│   │   └── index.html           # UI Command Center (Glassmorphism, Light/Dark, Telemetry Bar)
│   ├── static/
│   │   ├── css/style.css        # Responsive CSS Tokens, Custom Scroll, Status Animations
│   │   └── js/dashboard.js      # Chart.js Dual-Axis, Live Alert Fetcher, Auto-Refresh Interval
│   └── data/
│       ├── live_kurs.json       # Speed Layer Cache Kurs & Minyak
│       ├── live_pangan.json     # Speed Layer Cache Harga Pasar
│       └── gold/                # Artefak Analitik PySpark (7 File JSON Hasil Olahan)
├── docker-compose.yml           # Orkestrasi Multi-Kontainer: Kafka KRaft & Hadoop HDFS Cluster
├── hadoop.env                   # Variabel Lingkungan Hadoop NameNode & DataNode
├── requirements.txt             # Dependensi Pustaka Python
└── README.md                    # Dokumentasi Komprehensif Sistem
```

---

## 6. Fondasi Matematika & Algoritma AI

### A. PySpark Multi-Variate Linear Regression
Memproyeksikan harga pangan eceran ($\hat{Y}_t$) berdasarkan interaksi faktor nilai tukar rupiah ($X_{1,t}$) dan harga energi global ($X_{2,t}$):

$$\hat{Y}_t = \beta_0 + \beta_1 \cdot \text{Kurs}_t + \beta_2 \cdot \text{Oil}_t$$

Performa model dievaluasi secara transparan menggunakan **Root Mean Squared Error (RMSE)** dan **Koefisien Determinasi ($R^2$)**:
$$R^2 = 1 - \frac{\sum (Y_i - \hat{Y}_i)^2}{\sum (Y_i - \bar{Y})^2}$$

### B. Dynamic Time-Lag Analysis (Transmisi Shock)
Pelemahan kurs tidak seketika mentransmisikan dampak ke harga pasar di hari yang sama. Sistem menguji jeda waktu transmisi $k \in \{0, 7, 14, 21\text{ hari}\}$:

$$r_k = \text{Corr}(Y_t, X_{t-k})$$

Jeda waktu dengan nilai $|r_k|$ terbesar ditetapkan sebagai **Puncak Transmisi Shock** komoditas tersebut (misal: Bawang Putih Impor memiliki transmisi puncak pada lag 21 hari karena siklus kontrak impor, karantina, dan distribusi kontainer).

### C. Food Price Vulnerability Index (FPVI 2.0)
Indeks komposit berskala 0–100 untuk mengukur kerentanan komoditas terhadap guncangan eksternal secara terukur:

$$\text{FPVI} = \left( 0.40 \times W_{\text{impor}} \right) + \left( 0.35 \times \frac{|r_{\text{kurs}}| + |r_{\text{oil}}|}{2} \times 100 \right) + \left( 0.25 \times V_{\text{norm}} \right)$$

*Tingkat Kerentanan:* **Rendah** ($< 40$), **Sedang** ($40 - 70$), **Tinggi** ($> 70$).

### D. Residual-Based Anomaly Detection ($Z \ge 2.0$)
Membedakan kenaikan harga wajar (*cost-push*) dengan ulah penimbun/kartel menggunakan **Residual Analysis**:

$$e_t = Y_t - \hat{Y}_t, \quad Z_t = \frac{e_t - \mu_e}{\sigma_e}$$

* Jika $Z_t < 2.0$: Kenaikan harga selaras dengan tren kenaikan kurs dan BBM $\rightarrow$ Kebijakan: **Intervensi stabilisasi pasokan / subsidi logistik**.
* Jika $Z_t \ge 2.0$: Lonjakan harga tidak dapat dijelaskan oleh faktor makro $\rightarrow$ Kebijakan: **Audit pasokan & inspeksi satgas pangan terhadap indikasi kartel/penimbunan**.

---

## 7. Panduan Menjalankan Sistem (Production Runbook)

### Prasyarat:
* **Sistem Operasi:** macOS / Linux / Windows WSL2
* **Python Runtime:** Python 3.11+ dalam Virtual Environment
* **Docker & Docker Compose:** Klaster Apache Kafka dan Apache Hadoop HDFS
* **Java:** JDK 11+ (Runtime PySpark engine)

---

### Langkah 1: Persiapan Virtual Environment
Buka terminal di root direktori proyek:

```bash
# Inisialisasi dan aktivasi virtual environment
python3 -m venv .venv
source .venv/bin/activate       # macOS / Linux
# .\venv\Scripts\activate       # Windows PowerShell

# Instalasi seluruh pustaka dependensi
pip install -r requirements.txt
```

---

### Langkah 2: Menjalankan Klaster Big Data (Docker)
Inisialisasi kontainer Hadoop NameNode, DataNode, ResourceManager, NodeManager, dan Kafka KRaft:

```bash
docker-compose up -d
```

> ⏳ **Catatan:** Berikan jeda 20–30 detik agar NameNode selesai inisialisasi dan Kafka broker siap menerima koneksi pada port `9092`.

Verifikasi seluruh kontainer berjalan normal:
```bash
docker-compose ps
```

Berikan izin penulisan pada direktori root HDFS:
```bash
docker exec -it hadoop-namenode hdfs dfs -chmod -R 777 /
```

---

### Langkah 3: Ingesti Data Historis (Lakehouse Bronze Seeding)
Jalankan modul batch untuk mengisi HDFS Bronze dengan data historis riil:

```bash
python3 producers/batch_direct/fetch_kurs_bi.py
python3 producers/batch_direct/fetch_historical_bapanas.py
python3 producers/batch_direct/fetch_oil_eia.py
python3 producers/batch_direct/fetch_macro_bps.py
```

Verifikasi struktur direktori di HDFS:
```bash
docker exec -it hadoop-namenode hdfs dfs -ls /data/lakehouse/bronze/
```

---

### Langkah 4: Eksekusi Pipeline PySpark Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)
Jalankan layer transformasi data dan machine learning secara berurutan:

```bash
# 1. Transformasi Raw JSON ke Parquet Bronze
python3 spark/bronze_layer.py

# 2. Data Cleaning, Forward-Fill Windowing, & Join Multi-Dimensi
python3 spark/silver_layer.py

# 3. Model Regresi PySpark ML, Korelasi Lag, FPVI 2.0, & Deteksi Anomali
python3 spark/gold_layer.py
```

*Output:* Tujuh artefak analitik JSON tersimpan di direktori `dashboard/data/gold/`.

---

### Langkah 5: Menjalankan Speed Layer Consumer & Kafka Streaming Producers
Buka **3 terminal terpisah** (pastikan virtual environment aktif di setiap terminal):

* **Terminal 1 — Lakehouse Dual-Routing Consumer:**
  ```bash
  python3 consumers/consumer_to_hdfs.py
  ```
  *(Consumer mendengarkan Kafka, memperbarui cache lokal untuk Speed Layer, dan mem-flush micro-batch ke HDFS).*

* **Terminal 2 — Dual-Macro Real-Time Producer (Kurs & Minyak Dunia):**
  ```bash
  python3 producers/streaming_kafka/producer_kurs.py
  ```
  *(Streaming live spot USD/IDR dan Brent crude oil setiap 30 detik).*

* **Terminal 3 — Live Food Price Producer (Bapanas API):**
  ```bash
  python3 producers/streaming_kafka/producers_pangan.py
  ```
  *(Mengambil dan menyiarkan data harga pangan eceran Bapanas).*

---

### Langkah 6: Menjalankan Command Center Serving Dashboard
Buka **Terminal ke-4**, jalankan server web Flask:

```bash
python3 dashboard/app.py
```

Buka peramban (*browser*) Anda dan akses:
👉 **`http://localhost:8080`**

---

## 8. Maintainer & Lisensi

Platform ini dirancang, diarsiteki, dan dikembangkan secara independen oleh **Khumaidi Kharis Az-zacky** sebagai platform komputasi terdistribusi dan kecerdasan artifisial untuk ketahanan pangan nasional.
