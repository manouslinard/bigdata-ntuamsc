from pyspark.sql import SparkSession

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Read Crime Parquet Files") \
    .getOrCreate()

parquet_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_19"

# Dataframe:
crime_df = spark.read.parquet(parquet_path)
crime_df.printSchema()
crime_df.show(5)

# RDD:
crime_rdd = crime_df.rdd

print(crime_rdd.take(5))