from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pathlib import Path
from datetime import datetime

# --------------------------------------------------
# Spark session
# --------------------------------------------------
spark = (
    SparkSession.builder
    .appName("load-conti-csv")
    .getOrCreate()
)

# --------------------------------------------------
# Paths
# --------------------------------------------------
VALIDATED_PATH = "/opt/data/validated/conti"
OUTPUT_BASE_PATH = Path("/opt/data/output/conti")

OUTPUT_BASE_PATH.mkdir(parents=True, exist_ok=True)

run_date = datetime.now().strftime("%Y%m%d")
TMP_OUTPUT_PATH = f"{OUTPUT_BASE_PATH}/_tmp"
FINAL_CSV_PATH = f"{OUTPUT_BASE_PATH}/conti_{run_date}.csv"

# --------------------------------------------------
# 1. Read normalized parquet
# --------------------------------------------------
df = spark.read.parquet(VALIDATED_PATH)

# --------------------------------------------------
# 2. Ordering columns (IMPORTANTISSIMO)
# --------------------------------------------------
final_df = df.select(
    "id_conto",
    "iban",
    "saldo",
    "cod_filiale",
    "valuta",
    "last_update",
    "etl_load_ts"
)

# --------------------------------------------------
# 3. Write CSV (1 file)
# --------------------------------------------------
(
    final_df
    .coalesce(1)               # 🔴 fondamentale
    .write
    .mode("overwrite")
    .option("header", "true")
    .option("delimiter", ";")  # stile bancario
    .option("nullValue", "")
    .csv(TMP_OUTPUT_PATH)
)

# --------------------------------------------------
# 4. Rename file part-*.csv -> conti_YYYYMMDD.csv
# --------------------------------------------------
import os
import shutil

tmp_files = os.listdir(TMP_OUTPUT_PATH)
csv_file = [f for f in tmp_files if f.endswith(".csv")][0]

shutil.move(
    f"{TMP_OUTPUT_PATH}/{csv_file}",
    FINAL_CSV_PATH
)

shutil.rmtree(TMP_OUTPUT_PATH)

print(f"✅ CSV generated: {FINAL_CSV_PATH}")

spark.stop()
