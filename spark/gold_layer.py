"""
Gold Layer — SembakoWatch
Macro-to-Micro Food Security Intelligence & Early Warning Engine
- Agregasi Bulanan & Dynamic Time-Lag Analysis (Cross-Correlation)
- Multi-Variate Forecasting (PySpark ML Linear Regression: Kurs + Minyak)
- Residual-Based Anomaly Detection (Deteksi Penimbunan)
- Food Price Vulnerability Index (FPVI 2.0)
- HET Early Warning & Runway Calculation
"""

import os
import sys
import json
import numpy as np

os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, avg, round as spark_round,
    corr as spark_corr, lag, lit
)
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Import konfigurasi dan metadata resmi dari config
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.kafka_config import (
    HDFS_SILVER_PATH, GOLD_PATH, KOMODITAS_METADATA
)

LOCAL_GOLD_DIR = "dashboard/data/gold"
WEEKS_AHEAD = 4


# ─────────────────────────────────────────────────────────────────────────────
# 0. DYNAMIC TIME-LAG TRANSMISSION ANALYSIS (Cross-Correlation Harian)
# ─────────────────────────────────────────────────────────────────────────────
def compute_dynamic_time_lag(df_silver):
    """
    Menghitung lag hari (0, 7, 14, 21) dengan korelasi Pearson tertinggi
    antara kurs/minyak terhadap harga komoditas harian.
    """
    lag_days_candidates = [0, 7, 14, 21]
    w_kom = Window.partitionBy("komoditas").orderBy("date")

    df_lagged = df_silver
    for d in lag_days_candidates:
        if d > 0:
            df_lagged = df_lagged \
                .withColumn(f"kurs_lag_{d}", lag("rate", d).over(w_kom)) \
                .withColumn(f"oil_lag_{d}", lag("oil_price_usd", d).over(w_kom))
        else:
            df_lagged = df_lagged \
                .withColumn("kurs_lag_0", col("rate")) \
                .withColumn("oil_lag_0", col("oil_price_usd"))

    agg_exprs = []
    for d in lag_days_candidates:
        agg_exprs.append(spark_corr(f"kurs_lag_{d}", "harga").alias(f"corr_kurs_{d}"))
        agg_exprs.append(spark_corr(f"oil_lag_{d}", "harga").alias(f"corr_oil_{d}"))

    lag_rows = df_lagged.groupBy("komoditas").agg(*agg_exprs).collect()

    lag_map = {}
    for r in lag_rows:
        kom = r["komoditas"]
        best_lag = 0
        best_score = -1.0

        for d in lag_days_candidates:
            ck = abs(r[f"corr_kurs_{d}"] or 0.0)
            co = abs(r[f"corr_oil_{d}"] or 0.0)
            combined_score = (0.6 * ck) + (0.4 * co)
            if combined_score > best_score:
                best_score = combined_score
                best_lag = d

        lag_map[kom] = {
            "best_lag_days": best_lag,
            "max_lag_corr": round(best_score, 4)
        }
    return lag_map


# ─────────────────────────────────────────────────────────────────────────────
# 1. ML MULTI-VARIATE FORECASTING (Kurs + Minyak Dunia -> Harga Pangan)
# ─────────────────────────────────────────────────────────────────────────────
def run_multivariate_forecasting(monthly_joined, kurs_sorted, pangan_by_kom):
    print("\n[ML] ═══ Melatih Model Multi-Variate Forecasting (PySpark ML) ═══")

    if len(kurs_sorted) < 3:
        return {"model_info": {"status": "SKIPPED", "reason": "Data historis < 3 bulan"}, "forecasts": []}

    latest_kurs = kurs_sorted[-1]["avg_kurs"] or 15800.0
    latest_oil  = kurs_sorted[-1]["avg_oil_usd"] or 75.0

    # Gunakan window hingga 3 bulan terakhir agar tren mingguan lebih stabil
    lookback_idx = max(0, len(kurs_sorted) - 4)
    months_span  = max(1, len(kurs_sorted) - 1 - lookback_idx)

    base_kurs = kurs_sorted[lookback_idx]["avg_kurs"] or latest_kurs
    base_oil  = kurs_sorted[lookback_idx]["avg_oil_usd"] or latest_oil

    weekly_kurs_trend = (latest_kurs - base_kurs) / (months_span * 4.345)
    weekly_oil_trend  = (latest_oil - base_oil) / (months_span * 4.345)

    forecasts = []

    for kom in pangan_by_kom.keys():
        try:
            kom_df = monthly_joined.filter(col("komoditas") == kom) \
                .select(
                    col("avg_kurs").cast("double"),
                    col("avg_oil_usd").cast("double"),
                    col("avg_harga").cast("double")
                ).dropna()

            if kom_df.count() < 3:
                continue

            assembler = VectorAssembler(
                inputCols=["avg_kurs", "avg_oil_usd"],
                outputCol="features"
            )
            assembled_df = assembler.transform(kom_df)

            lr = LinearRegression(featuresCol="features", labelCol="avg_harga", maxIter=50, regParam=0.01)
            model = lr.fit(assembled_df)

            predictions = model.transform(assembled_df)
            evaluator_rmse = RegressionEvaluator(labelCol="avg_harga", predictionCol="prediction", metricName="rmse")
            evaluator_r2   = RegressionEvaluator(labelCol="avg_harga", predictionCol="prediction", metricName="r2")

            rmse = evaluator_rmse.evaluate(predictions)
            r2   = max(0.0, evaluator_r2.evaluate(predictions))

            coeff_kurs = float(model.coefficients[0])
            coeff_oil  = float(model.coefficients[1])
            intcpt     = float(model.intercept)

            current_price = pangan_by_kom[kom][-1]["avg_harga"] if pangan_by_kom.get(kom) else 0.0

            week_predictions = []
            for w in range(1, WEEKS_AHEAD + 1):
                proj_kurs  = latest_kurs + (weekly_kurs_trend * w)
                proj_oil   = max(20.0, latest_oil + (weekly_oil_trend * w))
                pred_price = max(1000.0, (coeff_kurs * proj_kurs) + (coeff_oil * proj_oil) + intcpt)

                week_predictions.append({
                    "week": w,
                    "predicted_price": round(pred_price),
                    "kurs_assumed": round(proj_kurs, 2),
                    "oil_assumed": round(proj_oil, 2)
                })

            forecasts.append({
                "komoditas": kom,
                "rmse": round(rmse, 2),
                "r2_score": round(r2, 4),
                "current_price": round(current_price),
                "coeff_kurs": round(coeff_kurs, 4),
                "coeff_oil": round(coeff_oil, 4),
                "intercept": round(intcpt, 2),
                "predictions": week_predictions
            })
            print(f"[ML]   {kom:25s} | R²={r2:.3f} | RMSE=Rp {rmse:,.0f} | Minggu-4: Rp {week_predictions[-1]['predicted_price']:,}")

        except Exception as e:
            print(f"[ML]   {kom}: Skip ({e})")
            continue

    return {
        "model_info": {
            "algorithm": "PySpark ML Multi-Variate Linear Regression",
            "features": ["avg_kurs", "avg_oil_usd"],
            "training_months": len(kurs_sorted),
            "status": "OK"
        },
        "forecasts": forecasts
    }


# ─────────────────────────────────────────────────────────────────────────────
# 2. RESIDUAL ANOMALY DETECTION (Memisahkan Efek Makro vs Penimbunan)
# ─────────────────────────────────────────────────────────────────────────────
def run_residual_anomaly_detection(kurs_sorted, pangan_by_kom):
    print("\n[ANOMALY] ═══ Menjalankan Deteksi Anomali Residu Makro ═══")

    kurs_lookup = {(d["year"], d["month"]): (d["avg_kurs"], d["avg_oil_usd"]) for d in kurs_sorted}
    all_anomalies = []

    for kom, series in pangan_by_kom.items():
        data_points = []
        for d in series:
            key = (d["year"], d["month"])
            if key in kurs_lookup and kurs_lookup[key][0] is not None:
                k_val, o_val = kurs_lookup[key]
                o_val = o_val or 75.0
                data_points.append((k_val, o_val, d["avg_harga"], d["year"], d["month"]))

        if len(data_points) < 3:
            continue

        kurs_arr  = np.array([p[0] for p in data_points])
        oil_arr   = np.array([p[1] for p in data_points])
        harga_arr = np.array([p[2] for p in data_points])

        X = np.column_stack([kurs_arr, oil_arr, np.ones(len(kurs_arr))])
        try:
            coeffs, _, _, _ = np.linalg.lstsq(X, harga_arr, rcond=None)
            expected_prices = np.dot(X, coeffs)
        except Exception:
            expected_prices = harga_arr

        residuals = harga_arr - expected_prices
        mean_res  = np.mean(residuals)
        std_res   = np.std(residuals)

        if std_res < 1e-6:
            continue

        z_scores = (residuals - mean_res) / std_res

        for i, pt in enumerate(data_points):
            z = float(z_scores[i])
            # Fokus pada lonjakan harga positif (z >= 1.8) atau penyimpangan ekstrem
            if abs(z) >= 1.8 and expected_prices[i] > 0:
                deviasi_pct = round(100 * (harga_arr[i] - expected_prices[i]) / expected_prices[i], 1)

                if z >= 2.5:
                    severity = "HIGH"
                    sebab = "Lonjakan harga ekstrem di luar pengaruh makro (Indikasi Penimbunan / Kartel Pasar)"
                elif z >= 1.8:
                    severity = "MEDIUM"
                    sebab = "Penyimpangan signifikan di atas tren makro (Indikasi Gangguan Distribusi Lokal)"
                else:
                    severity = "LOW"
                    sebab = "Anomali penurunan harga tajam di bawah ekspektasi makro (Panen Raya / Banjir Pasokan)"

                all_anomalies.append({
                    "komoditas": kom,
                    "bulan": f"{pt[3]:04d}-{pt[4]:02d}",
                    "harga_aktual": round(harga_arr[i]),
                    "harga_expected": round(expected_prices[i]),
                    "deviasi_persen": deviasi_pct,
                    "z_score": round(abs(z), 2),
                    "severity": severity,
                    "kemungkinan_penyebab": sebab
                })

    all_anomalies.sort(key=lambda x: -x["z_score"])
    print(f"[ANOMALY] Terdeteksi {len(all_anomalies)} anomali pasar.")

    return {
        "anomalies": all_anomalies,
        "summary": {
            "total_anomalies": len(all_anomalies),
            "high_severity": sum(1 for a in all_anomalies if a["severity"] == "HIGH"),
            "medium_severity": sum(1 for a in all_anomalies if a["severity"] == "MEDIUM")
        }
    }


# ─────────────────────────────────────────────────────────────────────────────
# 3. FPVI 2.0 & TIME-LAG TRANSMISSION ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
def compute_fpvi_and_lag(pangan_by_kom, corr_data, lag_map):
    results = []
    vols = [float(np.std([d["avg_harga"] for d in s])) if len(s) >= 2 else 0.0 for s in pangan_by_kom.values()]
    vmin, vmax = (min(vols), max(vols)) if vols else (0.0, 1.0)

    for r in corr_data:
        kom = r["komoditas"]
        meta = KOMODITAS_METADATA.get(kom, {"bobot_impor": 0.3, "kategori": "Umum"})

        series = pangan_by_kom.get(kom, [])
        vol = float(np.std([d["avg_harga"] for d in series])) if len(series) >= 2 else 0.0
        norm_vol = 0.0 if vmax == vmin else (vol - vmin) / (vmax - vmin)

        c_kurs = abs(r["pearson_corr_kurs"])
        c_oil  = abs(r.get("pearson_corr_oil") or 0.0)
        b_imp  = meta["bobot_impor"]

        # Formula FPVI 2.0
        fpvi_score = round(100 * (0.35 * c_kurs + 0.25 * c_oil + 0.20 * norm_vol + 0.20 * b_imp), 1)
        level = "Tinggi" if fpvi_score >= 65 else "Sedang" if fpvi_score >= 35 else "Rendah"

        # Gunakan hasil Dynamic Lag aktual dari Silver Layer jika tersedia
        lag_info = lag_map.get(kom)
        if lag_info and lag_info["max_lag_corr"] > 0.15:
            days = lag_info["best_lag_days"]
            if days == 0:
                peak_lag = "0 Hari (Respons Instan / Pasar Lokal)"
            elif days == 7:
                peak_lag = "7 Hari (Transmisi Distribusi Mingguan)"
            elif days == 14:
                peak_lag = "14 Hari (Transmisi Valas & Stok Importir)"
            else:
                peak_lag = "21 Hari (Siklus Pakan, Logistik & Manufaktur)"
        else:
            if b_imp >= 0.8:
                peak_lag = "14 Hari (Estimasi Transmisi Valas Langsung)"
            elif b_imp >= 0.5:
                peak_lag = "21 Hari (Estimasi Siklus Pakan & Logistik)"
            else:
                peak_lag = "0–7 Hari (Didominasi Musim Lokal)"

        latest_price = series[-1]["avg_harga"] if series else 0.0

        results.append({
            "komoditas": kom,
            "kategori": meta["kategori"],
            "fpvi_score": fpvi_score,
            "fpvi_level": level,
            "peak_transmission_lag": peak_lag,
            "optimal_lag_days": lag_info["best_lag_days"] if lag_info else None,
            "harga_terakhir": round(latest_price),
            "corr_kurs": r["pearson_corr_kurs"],
            "corr_oil": r.get("pearson_corr_oil")
        })

    results.sort(key=lambda x: -x["fpvi_score"])
    return {"komoditas": results}


# ─────────────────────────────────────────────────────────────────────────────
# 4. HET EARLY WARNING & RUNWAY SYSTEM
# ─────────────────────────────────────────────────────────────────────────────
def run_het_warning(pangan_by_kom, forecast_data):
    warnings = []
    fc_lookup = {f["komoditas"]: f for f in forecast_data.get("forecasts", [])}

    for kom, meta in KOMODITAS_METADATA.items():
        het = meta["het"]
        series = pangan_by_kom.get(kom, [])
        if not series or not het:
            continue

        latest_price = series[-1]["avg_harga"]
        pct_of_het = round(100 * (latest_price / het), 1)

        if pct_of_het >= 120:
            status = "DARURAT"
        elif pct_of_het >= 100:
            status = "KRITIS (Melanggar HET)"
        elif pct_of_het >= 80:
            status = "WASPADA"
        else:
            status = "AMAN"

        runway_week = None
        fc = fc_lookup.get(kom)
        if fc and fc.get("predictions"):
            for pred in fc["predictions"]:
                if pred["predicted_price"] >= het:
                    runway_week = pred["week"]
                    break

        warnings.append({
            "komoditas": kom,
            "harga_terakhir": round(latest_price),
            "het": het,
            "satuan": meta["satuan"],
            "persen_dari_het": pct_of_het,
            "status": status,
            "proyeksi_jebol_het": runway_week is not None,
            "estimasi_minggu_jebol": runway_week
        })

    return {"warnings": warnings}


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE UTAMA (MAIN EXECUTION)
# ─────────────────────────────────────────────────────────────────────────────
def main():
    spark = SparkSession.builder \
        .appName("GoldLayer-AI-SembakoWatch") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("\n[GOLD] ══════════════════════════════════════════════════════")
    print("[GOLD] MEMULAI PEMROSESAN ANALITIK & MACHINE LEARNING (GOLD)")
    print("[GOLD] ══════════════════════════════════════════════════════")

    # 1. BACA DATA DARI SILVER LAYER
    df = spark.read.parquet(f"{HDFS_SILVER_PATH}/kurs_pangan")
    df = df.withColumn("year", year(col("date"))).withColumn("month", month(col("date")))

    # Hitung Dynamic Time-Lag sebelum agregasi bulanan
    lag_map = compute_dynamic_time_lag(df)

    # 2. AGREGASI BULANAN
    monthly_kurs = df.groupBy("year", "month").agg(
        spark_round(avg("rate"), 2).alias("avg_kurs"),
        spark_round(avg("oil_price_usd"), 2).alias("avg_oil_usd")
    ).orderBy("year", "month")

    monthly_pangan = df.groupBy("year", "month", "komoditas").agg(
        spark_round(avg("harga"), 0).alias("avg_harga")
    ).orderBy("year", "month", "komoditas")

    monthly_joined = monthly_kurs.join(monthly_pangan, on=["year", "month"], how="inner")

    # Simpan tabel agregasi bulanan ke HDFS Gold Layer
    monthly_joined.write.mode("overwrite").parquet(f"{GOLD_PATH}/monthly_macro_pangan")

    # 3. KORELASI PEARSON (KURS & MINYAK)
    corr_df = monthly_joined.groupBy("komoditas").agg(
        spark_round(spark_corr("avg_kurs", "avg_harga"), 4).alias("pearson_corr_kurs"),
        spark_round(spark_corr("avg_oil_usd", "avg_harga"), 4).alias("pearson_corr_oil")
    ).orderBy("komoditas")

    # 4. EKSTRAKSI KE MEMORI UNTUK MODUL AI
    os.makedirs(LOCAL_GOLD_DIR, exist_ok=True)

    kurs_sorted = [
        {"year": r["year"], "month": r["month"], "avg_kurs": r["avg_kurs"], "avg_oil_usd": r["avg_oil_usd"]}
        for r in monthly_kurs.collect()
    ]
    with open(f"{LOCAL_GOLD_DIR}/monthly_kurs.json", "w", encoding="utf-8") as f:
        json.dump(kurs_sorted, f, indent=2)

    pangan_by_kom = {}
    for r in monthly_pangan.collect():
        pangan_by_kom.setdefault(r["komoditas"], []).append({
            "year": r["year"], "month": r["month"], "avg_harga": float(r["avg_harga"])
        })
    with open(f"{LOCAL_GOLD_DIR}/monthly_pangan.json", "w", encoding="utf-8") as f:
        json.dump(pangan_by_kom, f, indent=2)

    corr_data = [
        {
            "komoditas": r["komoditas"],
            "pearson_corr_kurs": float(r["pearson_corr_kurs"] or 0.0),
            "pearson_corr_oil": float(r["pearson_corr_oil"]) if r["pearson_corr_oil"] is not None else None
        }
        for r in corr_df.collect()
    ]
    with open(f"{LOCAL_GOLD_DIR}/correlation.json", "w", encoding="utf-8") as f:
        json.dump(corr_data, f, indent=2)

    # 5. JALANKAN 4 MODUL AI & EXPORT JSON
    forecast_data = run_multivariate_forecasting(monthly_joined, kurs_sorted, pangan_by_kom)
    with open(f"{LOCAL_GOLD_DIR}/forecast.json", "w", encoding="utf-8") as f:
        json.dump(forecast_data, f, indent=2)

    anomaly_data = run_residual_anomaly_detection(kurs_sorted, pangan_by_kom)
    with open(f"{LOCAL_GOLD_DIR}/anomaly.json", "w", encoding="utf-8") as f:
        json.dump(anomaly_data, f, indent=2)

    fpvi_data = compute_fpvi_and_lag(pangan_by_kom, corr_data, lag_map)
    with open(f"{LOCAL_GOLD_DIR}/analytics.json", "w", encoding="utf-8") as f:
        json.dump(fpvi_data, f, indent=2)

    het_data = run_het_warning(pangan_by_kom, forecast_data)
    with open(f"{LOCAL_GOLD_DIR}/het_warning.json", "w", encoding="utf-8") as f:
        json.dump(het_data, f, indent=2)

    # Simpan ringkasan FPVI & HET ke HDFS Gold Parquet untuk konsumsi downstream
    try:
        if fpvi_data.get("komoditas"):
            spark.createDataFrame(fpvi_data["komoditas"]).write.mode("overwrite").parquet(f"{GOLD_PATH}/fpvi_analytics")
        if het_data.get("warnings"):
            # Ganti None dengan 0 (artinya 0 minggu / tidak jebol) agar PySpark tahu tipenya adalah Integer
            clean_warnings = [
                {**w, "estimasi_minggu_jebol": w["estimasi_minggu_jebol"] if w["estimasi_minggu_jebol"] is not None else 0}
                for w in het_data["warnings"]
            ]
            spark.createDataFrame(clean_warnings).write.mode("overwrite").parquet(f"{GOLD_PATH}/het_warnings")
    except Exception as e:
        print(f"[GOLD] Info: Arsip Parquet dilewati ({e})")

    print(f"\n✅ [GOLD] Sukses! Seluruh analitik AI diekspor ke: {LOCAL_GOLD_DIR}/")
    spark.stop()


if __name__ == "__main__":
    main()