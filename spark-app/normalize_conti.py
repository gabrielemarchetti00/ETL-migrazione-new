from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    LongType, StringType, DoubleType, TimestampType
)
from pathlib import Path

# --------------------------------------------------
# Spark session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("normalize-conti")
    .getOrCreate()
)

# --------------------------------------------------
# Paths
# --------------------------------------------------
RAW_BASE_PATH = Path("/opt/data/raw")
VALIDATED_PATH = "/opt/data/validated/conti"
REJECTS_PATH = "/opt/data/rejects/conti"

# --------------------------------------------------
# 1. Ultimo parquet RAW
# --------------------------------------------------
raw_parquets = list(RAW_BASE_PATH.glob("conti_*.parquet"))

if not raw_parquets:
    raise RuntimeError("Nessun parquet RAW trovato")

latest_raw = max(raw_parquets, key=lambda p: p.stat().st_mtime)
raw_path = str(latest_raw)

print(f"📥 Reading RAW parquet: {raw_path}")

raw_df = spark.read.parquet(raw_path)

raw_df.printSchema()

# --------------------------------------------------
# 2. NORMALIZZAZIONE
# --------------------------------------------------
normalized_df = (
    raw_df
    # cast
    .withColumn("id_conto", col("id_conto").cast(LongType()))
    .withColumn("iban", col("iban").cast(StringType()))
    .withColumn("saldo", col("saldo").cast(DoubleType()))
    .withColumn("cod_filiale", col("cod_filiale").cast(StringType()))
    .withColumn("last_update", col("last_update").cast(TimestampType()))

    # colonne target BPER (derivate)
    .withColumn("valuta", lit("EUR"))
    .withColumn("etl_load_ts", current_timestamp())
)

# --------------------------------------------------
# 3. VALIDAZIONE
# --------------------------------------------------
valid_df = normalized_df.filter(
    col("id_conto").isNotNull() &
    col("iban").isNotNull() &
    col("saldo").isNotNull()
)

rejects_df = normalized_df.subtract(valid_df)

print(f"✔ Valid rows: {valid_df.count()}")
print(f"⚠ Reject rows: {rejects_df.count()}")

# --------------------------------------------------
# 4. SCRITTURA
# --------------------------------------------------
valid_df.write.mode("overwrite").parquet(VALIDATED_PATH)
rejects_df.write.mode("overwrite").parquet(REJECTS_PATH)

print("✅ Normalization completed")

spark.stop()
