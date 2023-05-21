from pyspark.sql import SparkSession

datasetRootPath = "D:\\Documents\\Programming\\Python-Projects\\Clusterdata_2019_e\\"
spark = SparkSession.builder.appName('Google Borg Cluster Traces Analysis').master('local[*]').getOrCreate()

parDF = spark.read.parquet(datasetRootPath + "instance_usage-*.parquet.gz")
parDF.createOrReplaceTempView("ParquetTable")
spark.sql("SELECT * FROM ParquetTable").show(n=50)
