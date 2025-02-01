from pyspark.sql import SparkSession
from delta import *
import pyspark

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

folderFiles = "data/raw/*.csv"

df = (spark
      .read
      .csv(folderFiles, header=False, inferSchema=True,sep=";"))

df.write.format("delta") \
   .option("path", "data/bronze/patx") \
   .option("overwriteSchema", "true") \
   .save()

spark.stop()