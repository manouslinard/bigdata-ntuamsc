from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, when, to_timestamp

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 2 - SQL") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

save_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/crime_10_25/"

df = spark.read.parquet(save_dir)

df.createOrReplaceTempView("crime_data_init")
spark.sql("""
    CREATE OR REPLACE TEMPORARY VIEW crime_data AS
    SELECT `AREA NAME`, YEAR(TO_TIMESTAMP(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS YEAR, `Status Desc`
    FROM crime_data_init;
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW closed_crime AS
SELECT `AREA NAME`, YEAR, COUNT(*) AS Closed_Crimes 
FROM crime_data
WHERE `Status Desc` != 'UNK' AND `Status Desc` != 'Invest Cont'
GROUP BY `AREA NAME`, YEAR;
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW total_crime AS
SELECT `AREA NAME`, YEAR, COUNT(*) AS TOTAL from crime_data
GROUP BY `AREA NAME`, YEAR;
""")

spark.sql("""
CREATE OR REPLACE TEMPORARY VIEW joined_t AS
SELECT c.`AREA NAME` AS AREA, c.YEAR AS YEAR, (c.Closed_Crimes/t.TOTAL)*100 AS PERCENTAGE FROM
total_crime t JOIN closed_crime c ON t.`AREA NAME` = c.`AREA NAME` AND t.YEAR = c.YEAR
""")

results_sql = spark.sql("""
SELECT AREA, YEAR, PERCENTAGE from joined_t
ORDER BY YEAR, PERCENTAGE DESC;
""")

results = results_sql.collect()

counter_dict = {}
for res in results:
    counter = counter_dict.get(res.YEAR, 1)
    if counter <= 3:
        print((res.YEAR, res.AREA, res.PERCENTAGE, counter))
        counter_dict[res.YEAR] = counter + 1
