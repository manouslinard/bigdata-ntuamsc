from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull

username = "manousoslinardakis"
spark = SparkSession \
    .builder \
    .appName("Read Parquet Files") \
    .getOrCreate()

parquet_path = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/"

folders = ['crime_10_19', 'crime_20_25', 'mo_codes', 'census_10', 'la_income', 'lapd']

# Check dataframes (5 rows and null counts):
for f in folders:
    try:
        df = spark.read.parquet(parquet_path+f+'/')
        print(f"\n=== Checking parquet in folder '{f}': ===")
        print(f"Total rows: {df.count()}")
        df.show(5, truncate=False)
                
        print("Null value counts per column:")
        null_counts = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            if null_count > 0:
                null_counts.append((column, null_count))       
        if null_counts:
            for column, count in null_counts:
                print(f"  - {column}: {count} null values")
        else:
            print("  No null values found in any columns")
            
    except Exception as e:
        print(f"Error reading folder '{f}': {str(e)}")
        
    print("\n" + "="*50)