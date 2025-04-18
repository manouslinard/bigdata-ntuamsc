from pyspark.sql import SparkSession

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 2 - RDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)

rdd = df.rdd

def extract_year(year: str):
    # manual year extraction:
    return int(year.replace('/', ' ').split(' ')[2])

# count yearly closed cases per police station:
closed_cases = rdd.map(lambda x: x if ("UNK" != x['Status Desc'] and "Invest Cont" != x['Status Desc']) else None) \
                .filter(lambda x: x is not None)
closed_cases_per_station = closed_cases.map(lambda x: ((x['AREA NAME'], extract_year(x['DATE OCC'])), 1)).reduceByKey(lambda x, y: x + y)

# count yearly total cases per police station:
total_cases_per_station  = rdd.map(lambda x: ((x['AREA NAME'], extract_year(x['DATE OCC'])), 1)).reduceByKey(lambda x, y: x + y)

# calculate closed cases percentage:
joined_data = closed_cases_per_station.join(total_cases_per_station)
closed_cases_percentages = joined_data.map(lambda x: (x[0], (x[1][0] / x[1][1]) * 100))
# form: ((station, year), closed_case_percentage)

results = closed_cases_percentages.coalesce(1).collect()
results = sorted(results, key=lambda x: (x[0][1], -x[1]))   # could not sort in Spark due to timeout error...
counter_dict = {}
for res in results:
    counter = counter_dict.get(res[0][1], 1)
    if counter <= 3:
        print((res[0][1], res[0][0], res[1], counter))
        counter_dict[res[0][1]] = counter + 1
