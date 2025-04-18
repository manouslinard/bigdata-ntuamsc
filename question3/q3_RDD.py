from pyspark.sql import SparkSession

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 1 - RDD") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)

rdd = df.rdd

# Function to categorize age
def get_age_group(age):
    if age is None:
        return "unknown"
    age_val = int(age)
    if age_val < 18:
        return "children"
    elif 18 <= age_val <= 24:
        return "young adults"
    elif 25 <= age_val <= 64:
        return "adults"
    elif age_val > 64:
        return "elderly"
    else:
        return "unknown"


# selects only records with aggravated assault:
agg_assault = rdd.map(lambda x: x if "AGGRAVATED ASSAULT" in x['Crm Cd Desc'] else None).filter(lambda x: x is not None)

# transforms records to ("age_group", 1), where "age_group" the age group of individual/row, and 1 = counter.
grouped_age = agg_assault.map(lambda x: (get_age_group(x["Vict Age"]), 1))

# for item in grouped_age.coalesce(1).take(5):
#     print(item)

# combines by key ("age_group") and sums the counters per key:
rdd_result = grouped_age.reduceByKey(lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False)

for item in rdd_result.coalesce(1).collect():
    print(item)
