from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Save MO codes txt to parquet") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

mo_schema = StructType([
    StructField("Code", StringType()),
    StructField("Description", StringType())
])

# Read file:
mo_rdd = spark.sparkContext.textFile(f"hdfs://hdfs-namenode:9000/user/root/data/MO_codes.txt")

# Split each line by space:
parsed_rdd = mo_rdd.map(lambda line: line.strip().split(" ", 1))

# Convert to DataFrame:
mo_df = spark.createDataFrame(parsed_rdd, schema=mo_schema)

mo_df.show(5)  # Show first 5 rows

mo_df.write.mode("overwrite").parquet(output_dir + "mo_codes/")

print(f"Data saved as Parquet to {output_dir}mo_codes/")