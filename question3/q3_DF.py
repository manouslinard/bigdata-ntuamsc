from pyspark.sql import SparkSession, functions as F

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 1 - Dataframe without UDF") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)

# Filter for incidents involving serious bodily harm
aggr_assault = df.filter(F.col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))

# Define age groups using when-otherwise
age_groups = aggr_assault.withColumn("AGE_GROUP",
                          F.when(F.col("Vict Age") < 18, "children") \
                           .when((F.col("Vict Age") >= 18) & (F.col("Vict Age") <= 24), "young adults") \
                           .when((F.col("Vict Age") >= 25) & (F.col("Vict Age") <= 64), "adults") \
                           .when(F.col("Vict Age") > 64, "elderly") \
                           .otherwise("unknown"))

# Count by age group
result_df = age_groups.groupBy("AGE_GROUP").count()
result_df = result_df.orderBy("count", ascending=False)

result_df.show()
