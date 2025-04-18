from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, to_timestamp

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 2 - DataFrame") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)

# add year column (for the year() function to work):
df = df.withColumn(
    "Year",
    year(to_timestamp(col("DATE OCC"), "MM/dd/yyyy hh:mm:ss a"))
)

# add flag column "IsClosed" -> 1 means closed case:
df_isclosed = df.withColumn(
    "IsClosed",
    when((col("Status Desc") != "UNK") & (col("Status Desc") != "Invest Cont"), 1).otherwise(0)
)

closed_cases = df_isclosed.groupBy("AREA NAME", "Year").sum('IsClosed') \
    .withColumnRenamed("sum(IsClosed)", "Cases_Closed")

total_cases = df_isclosed.groupBy("AREA NAME", "Year").count() \
    .withColumnRenamed("count", "Total")

joinedDf = closed_cases.join(total_cases, ["AREA NAME", "Year"], "inner")

joinedDf = joinedDf.withColumn(
    "Percentage", (col("Cases_Closed") / col('Total')) * 100
).orderBy('Year', -col('Percentage'))

results = joinedDf.collect()

counter_dict = {}
for res in results:
    counter = counter_dict.get(res['Year'], 1)
    if counter <= 3:
        print((res["Year"], res["AREA NAME"], res["Percentage"], counter))
        counter_dict[res['Year']] = counter + 1
