from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Save Census 2010 csv to parque") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

census10_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Total Population", IntegerType()),
    StructField("Median Age", FloatType()),
    StructField("Total Males", IntegerType()),
    StructField("Total Females", IntegerType()),
    StructField("Total Households", StringType()),
    StructField("Average Household Size", FloatType())
])

# Load the DataFrame
census10 = spark.read.format('csv') \
    .options(header='true') \
    .schema(census10_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv")

census10.show(5)   # shows 5 first rows

# Save as Parquet
census10.write.mode("overwrite").parquet(output_dir+"census_10/")

print(f"Data saved as Parquet to {output_dir}census_10/")
