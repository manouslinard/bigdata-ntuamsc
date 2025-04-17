from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Save Crime csv to parque") \
    .getOrCreate()

# Set log level
spark.sparkContext.setLogLevel("ERROR")

# Get job ID and define output path
job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

# Create schema matching the crime data structure
crime_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", StringType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", FloatType()),
    StructField("LON", FloatType())
])

# Load the 1st crime data DataFrame
crime_df1 = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2010_2019.csv")

crime_df1.show(5)   # shows 5 first rows

# Save as Parquet
crime_df1.write.mode("overwrite").parquet(output_dir+"crime_10_19/")

print(f"Data saved as Parquet to {output_dir}crime_10_19/")


# Load the 2nd crime data DataFrame
crime_df2 = spark.read.format('csv') \
    .options(header='true') \
    .schema(crime_schema) \
    .load(f"hdfs://hdfs-namenode:9000/user/root/data/LA_Crime_Data_2020_2025.csv")

crime_df2.show(5)   # shows 5 first rows

# Save as Parquet
crime_df2.write.mode("overwrite").parquet(output_dir+"crime_20_25/")


print(f"Data saved as Parquet to {output_dir}crime_20_25/")