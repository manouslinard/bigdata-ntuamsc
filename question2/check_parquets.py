from pyspark.sql import SparkSession

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Read Parquet Files") \
    .getOrCreate()

parquet_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

folders = ['crime_10_19', 'crime_20_25', 'mo_codes', 'census_10', 'la_income', 'lapd']

# Check dataframes (5 rows):
for f in folders:
    df = spark.read.parquet(parquet_path+f+'/')
    print(f"Checking parquet in folder '{f}':")
    df.show(5)
