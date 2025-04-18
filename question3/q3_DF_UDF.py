from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 1 - Dataframe with UDF") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)


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

# Register the UDF -> returns string
categorize_age_udf = F.udf(get_age_group, StringType())

udf_result = df.filter(F.col("Crm Cd Desc").contains("AGGRAVATED ASSAULT")) \
              .withColumn("AGE_GROUP", categorize_age_udf(F.col("Vict Age"))) \
              .groupBy("AGE_GROUP") \
              .count() \
              .orderBy("count", ascending=False)

udf_result.show()
