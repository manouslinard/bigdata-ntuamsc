from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 3 - Datframe with parquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/census_10/"
df_census = spark.read.parquet(save_dir)

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/la_income/"
df_income = spark.read.parquet(save_dir)

# df_census = df_census.withColumn('Total Households', col('Total Households').cast(IntegerType()))

joined_data = df_income.join(df_census, 'Zip Code', 'inner')

median_inc_person = joined_data.withColumn('Median_Income', (col('Estimated Median Income')*col('Total Households'))/col('Total Population'))

median_inc_person = median_inc_person.select('Zip Code', 'Median_Income')

median_inc_person = median_inc_person.orderBy('Median_Income')

median_inc_person.show(n=median_inc_person.count(), truncate=False)  # shows all.
