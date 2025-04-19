from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Query 3 - Datframe with csv") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId

# Load DataFrame Census ======================================
census10_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Total Population", IntegerType()),
    StructField("Median Age", FloatType()),
    StructField("Total Males", IntegerType()),
    StructField("Total Females", IntegerType()),
    StructField("Total Households", IntegerType()),
    StructField("Average Household Size", FloatType())
])

load_path = "hdfs://hdfs-namenode:9000/user/root/data/2010_Census_Populations_by_Zip_Code.csv"
# load_path = 'data/2010_Census_Populations_by_Zip_Code_20250417.csv'

df_census = spark.read.format('csv') \
    .options(header='true') \
    .schema(census10_schema) \
    .load(load_path)
# ===============================================================

# Load DataFrame LA Income ======================================
lainc_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])

load_path = f"hdfs://hdfs-namenode:9000/user/root/data/LA_income_2015.csv"
# load_path = 'data/LA_income_2015.csv'

df_income = spark.read.format('csv') \
    .options(header='true') \
    .schema(lainc_schema) \
    .load(load_path)

# convert "$x,yz", where "x,yz" money to float xyz:
df_income = df_income.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(FloatType())
)

# ======================================

joined_data = df_income.join(df_census, 'Zip Code', 'inner')
joined_data.explain()

median_inc_person = joined_data.withColumn('Median_Income', (col('Estimated Median Income')*col('Total Households'))/col('Total Population'))

median_inc_person = median_inc_person.select('Zip Code', 'Median_Income')

median_inc_person = median_inc_person.orderBy('Median_Income')

median_inc_person.show(n=median_inc_person.count(), truncate=False)  # shows all.
