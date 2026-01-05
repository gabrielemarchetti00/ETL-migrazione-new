from pyspark.sql import SparkSession
from datetime import datetime

spark = SparkSession.builder.appName("ingest-conti").getOrCreate()

jdbc_url = "jdbc:postgresql://postgres:5432/etl_metadata"
jdbc_props = {
    "user": "etl",
    "password": "etl",
    "driver": "org.postgresql.Driver"
}

job_name = "INGEST_CONTI"
run_start = datetime.now()

# 1️⃣ Leggi watermark
watermark_df = spark.read.jdbc(
    jdbc_url,
    "(SELECT last_watermark FROM etl_control WHERE job_name = 'INGEST_CONTI') t",
    properties=jdbc_props
)

watermark = watermark_df.collect()[0][0]

# 2️⃣ Estrai dati incrementali
query = f"""
(
  SELECT *
  FROM popso_conti
  WHERE last_update > TIMESTAMP '{watermark}'
) src
"""

df = spark.read.jdbc(jdbc_url, query, properties=jdbc_props)

records = df.count()

# 3️⃣ Scrivi RAW Parquet
run_ts = run_start.strftime("%Y%m%d_%H%M%S")

df.write.mode("overwrite").parquet(
    f"/opt/data/raw/conti_{run_ts}.parquet"
)

# 4️⃣ Aggiorna audit
spark.createDataFrame(
    [(job_name, run_start, datetime.now(), records, "SUCCESS")],
    ["job_name", "start_ts", "end_ts", "records_extracted", "status"]
).write.jdbc(
    jdbc_url,
    "etl_run",
    mode="append",
    properties=jdbc_props
)

# 5️⃣ Aggiorna watermark
spark.createDataFrame(
    [(job_name, datetime.now())],
    ["job_name", "last_watermark"]
).write.jdbc(
    jdbc_url,
    "etl_control",
    mode="overwrite",
    properties=jdbc_props
)

spark.stop()
