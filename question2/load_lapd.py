from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Save LAPD csv to parque") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

lapd_schema = StructType([
    StructField("OBJECTID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", IntegerType()),
    StructField("x", FloatType()),
    StructField("y", FloatType())
])

# Load the DataFrame
lapd = spark.read.format('csv') \
    .options(header='true') \
    .schema(lapd_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/LA_Police_Stations.csv")

lapd.show(5)   # shows 5 first rows

# Save as Parquet
lapd.write.mode("overwrite").parquet(output_dir+"lapd/")

print(f"Data saved as Parquet to {output_dir}lapd/")
