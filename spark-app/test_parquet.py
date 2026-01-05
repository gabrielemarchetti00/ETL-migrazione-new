from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-parquet") \
    .getOrCreate()

data = [
    ("IT00TESTIBAN0000000000000", 100.50),
    ("IT00TESTIBAN0000000000001", 200.00)
]

df = spark.createDataFrame(data, ["iban", "saldo"])

df.write.mode("overwrite").parquet("/opt/data/raw/conti_test.parquet")

spark.stop()
