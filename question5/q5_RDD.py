from pyspark.sql import SparkSession

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 3 - RDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/census_10/"
df = spark.read.parquet(save_dir)
rdd_census = df.rdd

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/la_income/"
df = spark.read.parquet(save_dir)
rdd_income = df.rdd

# put the zipcode as key (for join later):
rdd_income_zipkey = rdd_income.map(lambda x: (x['Zip Code'], x['Estimated Median Income']))
rdd_census_zipkey = rdd_census.map(lambda x: (x['Zip Code'], (x['Total Population'], x['Total Households'])))

joined_data = rdd_census_zipkey.join(rdd_income_zipkey)
# format: (zip_code, ((population, total_households), median_income_perhouse))

# for item in joined_data.coalesce(1).collect():
#     print(item)

# calculates median income per person and zip code:
median_inc_person = joined_data.map(lambda x: (x[0], (int(x[1][0][1])*x[1][1])/x[1][0][0]))
# follows formula: (total_households * median_income_perhouse) / total_population

for item in median_inc_person.coalesce(1).collect():
    print(item)