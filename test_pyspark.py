from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test") \
    .master("local[1]") \
    .getOrCreate()

df = spark.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
df.show()
spark.stop()