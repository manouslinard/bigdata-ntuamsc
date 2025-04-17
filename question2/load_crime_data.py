from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("RDD query 1 execution") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Get job ID and define output path
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

# Create schema matching the crime data structure
crime_schema = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", IntegerType()),
    StructField("AREA", IntegerType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", IntegerType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", IntegerType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", FloatType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", FloatType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", FloatType()),
    StructField("Crm Cd 2", FloatType()),
    StructField("Crm Cd 3", FloatType()),
    StructField("Crm Cd 4", FloatType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", FloatType()),
    StructField("LON", FloatType())
])

# Load the crime data DataFrame
crime_df = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv")

# Save as Parquet
crime_df.write.mode("overwrite").parquet(output_dir)

print(f"Data saved as Parquet to {output_dir}")