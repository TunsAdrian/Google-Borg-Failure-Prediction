from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Twitter Sentiment Analysis').master('local[*]').getOrCreate()

parDF = spark.read.parquet(
    "D:\\Documents\\Programming\\Python-Projects\\Clusterdata_2019_e\\instance_usage-*.parquet.gz")
parDF.createOrReplaceTempView("ParquetTable")
spark.sql("SELECT * FROM ParquetTable").show(n=50)
