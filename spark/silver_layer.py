import os
os.environ["HADOOP_USER_NAME"] = "root"

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, to_date, last, lit
from pyspark.sql.window import Window

HDFS_BRONZE_KURS   = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/kurs"
HDFS_BRONZE_PANGAN = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/pangan"
HDFS_BRONZE_EIA    = "hdfs://localhost:8020/data/lakehouse/bronze_parquet/eia"
HDFS_SILVER_PATH   = "hdfs://localhost:8020/data/lakehouse/silver"


def transform_silver():
    spark = SparkSession.builder \
        .appName("SilverLayer-FoodPulse") \
        .master("local[*]") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print("\n[SILVER] ═══ Memulai Transformasi Silver Layer ═══")

    # 1. BACA DATA BRONZE PARQUET
    kurs_df   = spark.read.parquet(HDFS_BRONZE_KURS)
    pangan_df = spark.read.parquet(HDFS_BRONZE_PANGAN)

    # 2. STANDARISASI & CASTING TIPE DATA KURS
    kurs_silver = kurs_df \
        .withColumn("date", to_date(col("timestamp"))) \
        .withColumn("rate", col("rate").cast(DoubleType())) \
        .select("date", "rate") \
        .dropDuplicates(["date"])

    # 3. STANDARISASI & CASTING TIPE DATA PANGAN (BASE TABLE)
    # Memastikan kolom 'kategori' ikut terbawa jika tersedia
    if "kategori" in pangan_df.columns:
        pangan_silver = pangan_df \
            .withColumn("date", to_date(col("tanggal"))) \
            .withColumn("harga", col("harga").cast(IntegerType())) \
            .select("date", "komoditas", "kategori", "harga")
    else:
        pangan_silver = pangan_df \
            .withColumn("date", to_date(col("tanggal"))) \
            .withColumn("harga", col("harga").cast(IntegerType())) \
            .withColumn("kategori", lit("Umum")) \
            .select("date", "komoditas", "kategori", "harga")

    # 4. GABUNGKAN PANGAN DENGAN KURS (LEFT JOIN)
    # Agar transaksi pasar akhir pekan tidak terhapus
    joined_df = pangan_silver.join(kurs_silver, on="date", how="left")

    # 5. GABUNGKAN DENGAN HARGA MINYAK EIA BRENT (JIKA ADA)
    try:
        eia_df = spark.read.parquet(HDFS_BRONZE_EIA)
        eia_silver = eia_df \
            .withColumn("date", to_date(col("date"))) \
            .withColumn("oil_price_usd", col("price_usd").cast(DoubleType())) \
            .select("date", "oil_price_usd") \
            .dropDuplicates(["date"])

        joined_df = joined_df.join(eia_silver, on="date", how="left")
        print("[SILVER] Data Minyak Mentah Brent (EIA) berhasil di-join.")
    except Exception as e:
        print(f"[SILVER] Info: Data EIA tidak ditemukan/dilewati ({e})")
        joined_df = joined_df.withColumn("oil_price_usd", lit(None).cast(DoubleType()))

    # 6. FORWARD-FILL HANDLING (Imputasi Hari Libur Bursa/Bank)
    # Mengisi nilai kurs/minyak di akhir pekan menggunakan nilai hari kerja sebelumnya
    window_spec = Window.partitionBy("komoditas").orderBy("date").rowsBetween(Window.unboundedPreceding, 0)

    joined_df = joined_df \
        .withColumn("rate", last("rate", ignorenulls=True).over(window_spec)) \
        .withColumn("oil_price_usd", last("oil_price_usd", ignorenulls=True).over(window_spec))

    # 7. SIMPAN HASIL BERSIH KE HDFS SILVER (PARQUET)
    target_path = f"{HDFS_SILVER_PATH}/kurs_pangan"
    joined_df.write.mode("overwrite").parquet(target_path)

    print(f"✅ [SILVER] Sukses! Data bersih terintegrasi disimpan di: {target_path}\n")
    spark.stop()


if __name__ == "__main__":
    transform_silver()