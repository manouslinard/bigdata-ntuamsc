from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Save LA income csv to parque") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

lainc_schema = StructType([
    StructField("Zip Code", IntegerType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])

# Load the DataFrame
lainc = spark.read.format('csv') \
    .options(header='true') \
    .schema(lainc_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv")

lainc.show(5)   # shows 5 first rows

# Save as Parquet
lainc.write.mode("overwrite").parquet(output_dir+"la_income/")

print(f"Data saved as Parquet to {output_dir}la_income/")
