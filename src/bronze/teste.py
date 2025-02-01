from pyspark.sql import SparkSession
from delta import *
import pyspark

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

folderFiles = "data/raw/*.csv"

df = spark.read.format("delta").load("data/bronze/patx").where("_c3 = 'AFN'")
df.show()


