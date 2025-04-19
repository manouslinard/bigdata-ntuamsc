from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper, split, explode, sqrt, pow, cos

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 4 - Dataframe") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Get configuration details:
executor_instances = spark.conf.get("spark.executor.instances", "0")
executor_cores = spark.conf.get("spark.executor.cores", "0")
executor_memory = spark.conf.get("spark.executor.memory", "0")
# if executor_cores != "0" or executor_instances != "0" or executor_memory != "0":
print(f"Configuration: {executor_instances} executors × {executor_cores} cores/{executor_memory} memory")

job_id = spark.sparkContext.applicationId

df_lapd = spark.read.parquet(f'hdfs://hdfs-namenode:9000/user/{username}/data/parquet/lapd/')

df_lapd = df_lapd.withColumn("DIVISION", upper(col("DIVISION")))

# change some values of DIVISION to match crime_10_25:
df_lapd = df_lapd.withColumn(
    'AREA NAME',
    when(col('DIVISION') == 'NORTH HOLLYWOOD', 'N HOLLYWOOD')
     .when(col('DIVISION') == 'WEST LOS ANGELES', 'WEST LA')
     .otherwise(col('DIVISION'))
)
# select the columns we are interested from the lapd dataset:
df_lapd = df_lapd.select(col('X'), col('Y'), col('AREA NAME'))


df_crime = spark.read.parquet(f'hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/')

# select the columns we are interested from the crime dataset:
df_crime = df_crime.select(col('Mocodes'), col('LAT'), col('LON'), col('AREA NAME'))

# removes crime incidents that refer to Null Island (lat = 0, lon = 0):
df_crime = df_crime.filter(~((col("LAT") == 0) & (col("LON") == 0)))

# convert to uppercase for join later:
df_crime = df_crime.withColumn("AREA NAME", upper(col("AREA NAME")))

joined_data = df_crime.join(df_lapd, 'AREA NAME', 'inner')

# rename columns for better understanding (Latitude -> y, Longitude  -> X):
joined_data = (joined_data
    .withColumnRenamed("LAT", "Crime_LAT")
    .withColumnRenamed("LON", "Crime_LON")
    .withColumnRenamed("Y", "Police_LAT")
    .withColumnRenamed("X", "Police_LON")
)

# Mocodes column has multiple mocodes -> use explode() to split the values into separate rows:
df_explode = joined_data.withColumn("Mocode", explode(split("Mocodes", " ")))
# print(f"Number of rows: {df_explode.count()}")

df_mo = spark.read.parquet(f'hdfs://hdfs-namenode:9000/user/{username}/data/parquet/mo_codes/')

joined_data = df_explode.join(df_mo, df_explode.Mocode == df_mo.Code, 'inner')
# print(f"Number of rows: {joined_data.count()}")

# drops unwanted columns:
joined_data = joined_data.drop("Mocodes", "Mocode", "Code")

joined_data = joined_data.withColumn("Description", upper(col("Description")))

# keeps only rows/crimes that description contains gun or weapon:
joined_data = joined_data.filter(
    (col("Description").contains("GUN")) |
    (col("Description").contains("WEAPON"))
)

# calculated euclidean distance using following formula:
# https://jonisalonen.com/2014/computing-distance-between-coordinates-can-be-simple-and-fast/
joined_data = joined_data.withColumn(
    "Eucl_dist",
    110.25 * sqrt(
        pow(col("Crime_LAT") - col("Police_LAT"), 2) +
        pow((col("Crime_LON") - col("Police_LON")) * cos(col("Police_LAT")), 2)
    )
)

# Groups calculations:
grouped_dist = joined_data.groupBy('AREA NAME').avg('Eucl_dist')
# grouped_dist.show()
grouped_count = joined_data.groupBy('AREA NAME').count()
# grouped_count.show()

# joins the metrics:
grouped = grouped_dist.join(grouped_count, "AREA NAME", 'inner')

# order by count of crime indices (descending):
grouped = grouped.orderBy('count', ascending=False)

grouped = (grouped
    .withColumnRenamed("AREA NAME", "division")
    .withColumnRenamed("avg(Eucl_dist)", "average_distance")
    .withColumnRenamed("count", "#")
)

grouped.show(21)

# code to print null island coords (there is none now):
# print('crime:')
# joined_data.filter((col("Crime_LAT") == 0) & (col("Crime_LON") == 0)).show()
#
# print('police:')
# joined_data.filter((col("Police_LAT") == 0) & (col("Police_LON") == 0)).show()
